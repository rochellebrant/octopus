import sys, os
from unittest.mock import patch, MagicMock, call
from collections import namedtuple
from functools import wraps
import types
from io import StringIO

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
)
from pyspark.sql import Window
import pyspark.sql.functions as f
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up repo path
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from funcs.general_functions import *

# =============================================================================
#  UTILITIES & MOCKS
# =============================================================================

def suppress_print(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)
    return wrapper

def _fake_column(name: str) -> MagicMock:
    col_mock = MagicMock(name=f"col({name})")
    col_mock._expr = f"<expr for {name}>"      # satisfies Spark internals
    col_mock.isNull.return_value = MagicMock(name=f"isNull({name})", _expr="<isNull>")
    return col_mock

# A reusable fake-f namespace
fake_f = types.SimpleNamespace(
    to_timestamp=lambda x: MagicMock(name="to_timestamp", _expr="<to_ts>"),
    lit=lambda x: MagicMock(name=f"lit({x})", _expr=f"<lit {x}>"),
    when=lambda cond, val: MagicMock(name="when()", _expr="<when>"),
    col=_fake_column,
)

def _patched_fake_f():
    """
    Returns the previously-defined `fake_f` namespace but with an extra
    `row_number` attribute (a Column-like mock exposing .over() & _expr).
    """
    if not hasattr(fake_f, "row_number"):
        rn = MagicMock(name="row_number")
        rn._expr = "<row_number>"
        rn.over.side_effect = (
            lambda w: MagicMock(name="row_number_over", _expr="<row_number_over>")
        )
        setattr(fake_f, "row_number", rn)
    return fake_f

# window helper stub (orderBy returns a mock WindowSpec)
fake_w = types.SimpleNamespace(orderBy=lambda *cols: MagicMock(name="windowSpec"))


# =============================================================================
#  TESTS
# =============================================================================

@suppress_print
def test_init_empty_tables():
    spark = MagicMock()
    dummy_df = MagicMock()
    spark.sql.return_value = dummy_df

    table_config = {
        "table1": {"catalogue_schema": "schema1"},
        "table2": {"catalogue_schema": "schema2"},
    }
    tables = ["table1", "table2"]
    table_prefix = "prefix_"

    init_empty_tables(spark, table_config, tables, table_prefix)

    assert spark.sql.call_count == 3 * len(tables)

    calls = spark.sql.call_args_list
    expected_drop = f"DROP TABLE IF EXISTS {table_prefix}table1"
    expected_create = f"CREATE TABLE {table_prefix}table1"
    expected_grant = f"GRANT MANAGE ON {table_prefix}table1 TO `Sennen XIO Project`"

    drop_called = any(expected_drop in call_arg[0][0] for call_arg in calls)
    create_called = any(expected_create in call_arg[0][0] for call_arg in calls)
    grant_called = any(expected_grant in call_arg[0][0] for call_arg in calls)

    assert drop_called, "DROP TABLE statement not found for table1"
    assert create_called, "CREATE TABLE statement not found for table1"
    assert grant_called, "GRANT statement not found for table1"


@suppress_print
def test_init_empty_tables_single_success():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()           
    table_config = {"orders": {"catalogue_schema": "cat_db"}}
    tables = ["orders"]
    prefix = "tgt_"

    init_empty_tables(spark, table_config, tables, prefix)

    drop_q, create_q, grant_q = (c[0][0].strip() for c in spark.sql.call_args_list)

    assert drop_q.startswith("DROP TABLE IF EXISTS tgt_orders")
    assert "CREATE TABLE tgt_orders" in create_q and "LIKE cat_db.orders" in create_q
    assert "GRANT MANAGE ON tgt_orders" in grant_q
    assert spark.sql.call_count == 3, "Expected three SQL calls"


@suppress_print
def test_init_empty_tables_multi_success():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    table_config = {
        "t1": {"catalogue_schema": "cat"},
        "t2": {"catalogue_schema": "cat"},
    }
    tables = ["t1", "t2"]
    prefix = "p_"

    init_empty_tables(spark, table_config, tables, prefix)

    assert spark.sql.call_count == 6, "Should execute 6 statements for 2 tables"
    create_statements = [c[0][0] for c in spark.sql.call_args_list if "CREATE TABLE" in c[0][0]]
    assert any("p_t1" in stmt for stmt in create_statements)
    assert any("p_t2" in stmt for stmt in create_statements)


@suppress_print
def test_init_empty_tables_drop_failure():
    spark = MagicMock()
    spark.sql.side_effect = Exception("drop failed")   
    table_config = {"bad": {"catalogue_schema": "cat"}}
    tables = ["bad"]

    init_empty_tables(spark, table_config, tables, "x_")

    assert spark.sql.call_count == 1, "CREATE/GRANT should be skipped after drop failure"


@suppress_print
def test_init_empty_tables_create_failure():
    spark = MagicMock()
    spark.sql.side_effect = [None, Exception("create failed")]   
    table_config = {"t": {"catalogue_schema": "cat"}}

    init_empty_tables(spark, table_config, ["t"], "x_")

    assert spark.sql.call_count == 2, "GRANT should not be attempted on create failure"


@suppress_print
def test_init_empty_tables_missing_config():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    table_config = {}               
    tables = ["ghost"]

    init_empty_tables(spark, table_config, tables, "p_")

    assert spark.sql.call_count == 1, "Only DROP should run when config is missing"
    drop_stmt = spark.sql.call_args_list[0][0][0]
    assert "DROP TABLE IF EXISTS p_ghost" in drop_stmt


@suppress_print
def test_data_loader_single_day_success():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    cfg = {
        "orders": {
            "catalogue_schema": "cat",
            "load_date_column": "load_ts",
            "match_keys": ["id", "region"],
        }
    }

    data_loader(
        spark=spark,
        table_config=cfg,
        tables=["orders"],
        table_prefix="tgt_",
        filter_date="2025-06-30",
        mode="single_day",
    )

    spark.sql.assert_called_once()
    sql_text = spark.sql.call_args[0][0]
    assert "MERGE INTO tgt_orders" in sql_text
    assert "cat.orders" in sql_text
    assert "CAST(load_ts as timestamp) = '2025-06-30'" in sql_text
    assert "tgt.id = src.id AND tgt.region = src.region" in sql_text


@suppress_print
def test_data_loader_multi_day_two_tables():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    cfg = {
        "t1": {
            "catalogue_schema": "c1",
            "load_date_column": "ld",
            "match_keys": ["k"],
        },
        "t2": {
            "catalogue_schema": "c2",
            "load_date_column": "ld",
            "match_keys": ["k"],
        },
    }

    data_loader(
        spark,
        table_config=cfg,
        tables=["t1", "t2"],
        table_prefix="p_",
        filter_date="2025-01-01",
        mode="multi_day",
    )

    assert spark.sql.call_count == 2, "Expected one MERGE per table"
    for call_ in spark.sql.call_args_list:
        assert ">='2025-01-01'".replace("'>", "' >")[:-1] in call_[0][0].replace(" ", ""), (
            "Operator >= expected in filter"
        )


@suppress_print
def test_data_loader_invalid_mode():
    spark = MagicMock()
    data_loader(
        spark,
        table_config={},
        tables=[],
        table_prefix="p_",
        filter_date="2025-01-01",
        mode="yesterday",              
    )

    spark.sql.assert_not_called()


@suppress_print
def test_data_loader_table_not_in_config():
    spark = MagicMock()
    try:
        data_loader(
            spark,
            table_config={},           
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-01-01",
        )
        assert False, "Expected KeyError for missing table_config entry"
    except KeyError:
        pass


@suppress_print
def test_data_loader_missing_match_keys():
    spark = MagicMock()
    cfg = {
        "err_tbl": {
            "catalogue_schema": "c",
            "load_date_column": "ld",
        }
    }

    try:
        data_loader(
            spark,
            table_config=cfg,
            tables=["err_tbl"],
            table_prefix="p_",
            filter_date="2025-01-01",
        )
        assert False, "Expected KeyError due to missing match_keys"
    except KeyError:
        pass


@suppress_print
def test_data_loader_empty_tables():
    spark = MagicMock()
    data_loader(
        spark,
        table_config={},
        tables=[],
        table_prefix="p_",
        filter_date="2025-01-01",
    )

    spark.sql.assert_not_called()


@suppress_print
def test_duplicate_data_loader_append_single_success():
    spark = MagicMock()
    df_mock = MagicMock(name="DataFrame")
    df_mock.columns = ["c1", "c2", "dt_col"]

    df_write = MagicMock(name="WriteObj")
    df_write_mode = MagicMock(name="ModeObj")
    df_write.mode.return_value = df_write_mode
    df_write_mode.saveAsTable.return_value = None
    df_mock.write = df_write
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {"orders": {"load_date_column": "dt_col"}}

    with patch("funcs.general_functions.f", fake_f):
        duplicate_data_loader_append(
            spark,
            table_config=cfg,
            tables=["orders"],
            table_prefix="tgt_",
            filter_date="2025-06-30",
            overwrite_date="2025-07-01",
        )

    spark.table.assert_called_once_with("tgt_orders")
    spark.sql.assert_called_once()
    df_mock.withColumn.assert_called_once()          
    df_mock.select.assert_called_once_with(*df_mock.columns)
    df_write.mode.assert_called_once_with("append")
    df_write_mode.saveAsTable.assert_called_once_with("tgt_orders")


@suppress_print
def test_duplicate_data_loader_append_same_dates_early_exit():
    spark = MagicMock()
    duplicate_data_loader_append(
        spark,
        table_config={},
        tables=["tbl"],
        table_prefix="p_",
        filter_date="2025-01-01",
        overwrite_date="2025-01-01",
    )

    spark.table.assert_not_called()
    spark.sql.assert_not_called()


@suppress_print
def test_duplicate_data_loader_append_multi_tables():
    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["col", "dt"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    write_obj = MagicMock()
    write_mode = MagicMock()
    write_obj.mode.return_value = write_mode
    df_mock.write = write_obj
    write_mode.saveAsTable.return_value = None

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {
        "t1": {"load_date_column": "dt"},
        "t2": {"load_date_column": "dt"},
    }

    with patch("funcs.general_functions.f", fake_f):
        duplicate_data_loader_append(
            spark,
            table_config=cfg,
            tables=["t1", "t2"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )

    assert spark.table.call_count == 2
    assert spark.sql.call_count == 2
    assert write_mode.saveAsTable.call_count == 2


@suppress_print
def test_duplicate_data_loader_append_missing_config():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])

    try:
        duplicate_data_loader_append(
            spark,
            table_config={},            
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )
        assert False, "Expected KeyError due to missing config entry"
    except KeyError:
        pass


@suppress_print
def test_duplicate_data_loader_append_sql_failure():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("boom")

    cfg = {"tbl": {"load_date_column": "d"}}

    try:
        with patch("funcs.general_functions.f", fake_f):
            duplicate_data_loader_append(
                spark,
                table_config=cfg,
                tables=["tbl"],
                table_prefix="p_",
                filter_date="2025-06-01",
                overwrite_date="2025-07-01",
            )
        assert False, "Expected upstream exception to propagate"
    except Exception as e:
        assert "boom" in str(e)


def test_duplicate_data_loader_append_different_dates():
    spark = MagicMock()
    table_config = {"table1": {"load_date_column": "load_date"}}
    tables = ["table1"]
    table_prefix = "prefix_"
    filter_date = "2023-01-01"
    overwrite_date = "2023-01-02"

    dummy_df = MagicMock()
    dummy_df.columns = ["col1", "load_date", "col3"]
    dummy_df.withColumn.return_value = dummy_df
    dummy_df.select.return_value = dummy_df
    dummy_df.write.mode.return_value = dummy_df

    spark.table.return_value = dummy_df
    spark.sql.return_value = dummy_df

    duplicate_data_loader_append(
        spark, table_config, tables, table_prefix, filter_date, overwrite_date
    )

    spark.sql.assert_called_once()
    call_sql = spark.sql.call_args[0][0]
    assert "SELECT *" in call_sql

    dummy_df.write.mode("append").saveAsTable.assert_called_once_with(
        f"{table_prefix}table1"
    )


@suppress_print
def test_new_data_loader_kpis_success():
    spark = MagicMock()
    df_mock = MagicMock(name="DataFrame")
    df_mock.columns = ["id", "k1", "load_dt"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    df_mock.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {
        "orders": {
            "load_date_column": "load_dt",
            "type": "kpis",
            "kpi_keys": ["k1"],
        }
    }

    with patch("funcs.general_functions.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["orders"],
            table_prefix="tgt_",
            filter_date="2025-06-30",
            overwrite_date="2025-07-01",
        )

    spark.table.assert_called_once_with("tgt_orders")
    spark.sql.assert_called_once()
    assert df_mock.withColumn.call_count == 2
    df_mock.write.mode.assert_called_once_with("append")
    df_mock.write.mode.return_value.saveAsTable.assert_called_once_with("tgt_orders")


@suppress_print
def test_new_data_loader_non_kpis_success():
    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["id", "kpi_a", "dt"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    df_mock.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {
        "tbl": {
            "load_date_column": "dt",
            "type": "other",
            "kpi_keys": ["kpi_a"],
        }
    }

    with patch("funcs.general_functions.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["tbl"],
            table_prefix="p_",
            filter_date="2025-05-01",
            overwrite_date="2025-05-02",
        )

    spark.table.assert_called_once_with("p_tbl")
    assert df_mock.withColumn.call_count == 2
    df_mock.write.mode.assert_called_once_with("append")


@suppress_print
def test_new_data_loader_same_dates():
    spark = MagicMock()
    new_data_loader(
        spark,
        table_config={},
        tables=["x"],
        table_prefix="p_",
        filter_date="2025-01-01",
        overwrite_date="2025-01-01",
    )

    spark.table.assert_not_called()
    spark.sql.assert_not_called()


@suppress_print
def test_new_data_loader_multi_tables():
    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["id", "kpi", "d"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    write_obj = MagicMock()
    write_mode = MagicMock()
    write_obj.mode.return_value = write_mode
    write_mode.saveAsTable.return_value = None
    df_mock.write = write_obj

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {
        "t1": {
            "load_date_column": "d",
            "type": "kpis",
            "kpi_keys": ["kpi"],
        },
        "t2": {
            "load_date_column": "d",
            "type": "other",
            "kpi_keys": ["kpi"],
        },
    }

    with patch("funcs.general_functions.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["t1", "t2"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )

    assert spark.table.call_count == 2
    assert spark.sql.call_count == 2
    assert write_mode.saveAsTable.call_count == 2


@suppress_print
def test_new_data_loader_missing_config():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c"])

    try:
        new_data_loader(
            spark,
            table_config={},            
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )
        assert False, "Expected KeyError for missing config"
    except KeyError:
        pass


@suppress_print
def test_new_data_loader_sql_failure():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("SQL failure")

    cfg = {"tbl": {"load_date_column": "d", "type": "kpis", "kpi_keys": []}}

    try:
        with patch("funcs.general_functions.f", fake_f):
            new_data_loader(
                spark,
                table_config=cfg,
                tables=["tbl"],
                table_prefix="p_",
                filter_date="2025-06-01",
                overwrite_date="2025-07-01",
            )
        assert False, "Expected exception to propagate"
    except Exception as e:
        assert "SQL failure" in str(e)


@suppress_print
def test_new_data_loader_empty_kpi_list():
    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["id", "dt"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    df_mock.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    cfg = {
        "tbl": {
            "load_date_column": "dt",
            "type": "kpis",
            "kpi_keys": [],    
        }
    }

    with patch("funcs.general_functions.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["tbl"],
            table_prefix="p_",
            filter_date="2025-01-01",
            overwrite_date="2025-02-01",
        )

    assert df_mock.withColumn.call_count == 1


@suppress_print
def test_mixed_data_loader_single_kpi_success():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "k1", "load_dt"]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.drop.return_value = df
    df.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df
    spark.sql.return_value = df

    cfg = {
        "orders": {
            "load_date_column": "load_dt",
            "kpi_keys": ["k1"],
        }
    }

    with patch("funcs.general_functions.f", _patched_fake_f()), patch("funcs.general_functions.w", fake_w):
        mixed_data_loader(
            spark,
            table_config=cfg,
            tables=["orders"],
            table_prefix="tgt_",
            filter_date="2025-06-30",
            overwrite_date="2025-07-01",
        )

    spark.table.assert_called_once_with("tgt_orders")
    spark.sql.assert_called_once()
    assert df.withColumn.call_count == 3, "Expected 3 withColumn calls"
    df.drop.assert_called_once_with("row_number")
    df.write.mode.assert_called_once_with("append")
    df.write.mode.return_value.saveAsTable.assert_called_once_with("tgt_orders")


@suppress_print
def test_mixed_data_loader_same_dates_exit():
    spark = MagicMock()
    mixed_data_loader(
        spark,
        table_config={},
        tables=["tbl"],
        table_prefix="p_",
        filter_date="2025-01-01",
        overwrite_date="2025-01-01",
    )

    spark.table.assert_not_called()
    spark.sql.assert_not_called()


@suppress_print
def test_mixed_data_loader_multi_tables_multi_kpis():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["a", "k1", "k2", "dt"]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.drop.return_value = df
    df.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df
    spark.sql.return_value = df

    cfg = {
        "t1": {"load_date_column": "dt", "kpi_keys": ["k1", "k2"]},
        "t2": {"load_date_column": "dt", "kpi_keys": ["k1", "k2"]},
    }

    with patch("funcs.general_functions.f", _patched_fake_f()), patch("funcs.general_functions.w", fake_w):
        mixed_data_loader(
            spark,
            table_config=cfg,
            tables=["t1", "t2"],
            table_prefix="p_",
            filter_date="2025-02-01",
            overwrite_date="2025-03-01",
        )

    N, kpis = 2, 2
    expected_withcols = N * (1 + 2 * kpis)
    assert df.withColumn.call_count == expected_withcols
    assert spark.table.call_count == N
    assert spark.sql.call_count == N
    assert df.write.mode.return_value.saveAsTable.call_count == N


@suppress_print
def test_mixed_data_loader_missing_config():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c"])

    try:
        mixed_data_loader(
            spark,
            table_config={},
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-04-01",
            overwrite_date="2025-05-01",
        )
        assert False, "Expected KeyError"
    except KeyError:
        pass


@suppress_print
def test_mixed_data_loader_sql_failure():
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("sql boom")

    cfg = {"tbl": {"load_date_column": "d", "kpi_keys": []}}

    with patch("funcs.general_functions.f", _patched_fake_f()), patch("funcs.general_functions.w", fake_w):
        try:
            mixed_data_loader(
                spark,
                table_config=cfg,
                tables=["tbl"],
                table_prefix="p_",
                filter_date="2025-04-01",
                overwrite_date="2025-05-01",
            )
            assert False, "Expected propagated exception"
        except Exception as e:
            assert "sql boom" in str(e)


@suppress_print
def test_mixed_data_loader_empty_kpi_list():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "dt"]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df
    spark.sql.return_value = df

    cfg = {"tbl": {"load_date_column": "dt", "kpi_keys": []}}

    with patch("funcs.general_functions.f", _patched_fake_f()), patch("funcs.general_functions.w", fake_w):
        mixed_data_loader(
            spark,
            table_config=cfg,
            tables=["tbl"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )

    assert df.withColumn.call_count == 1, "Only load_date overwrite expected"
    df.select.assert_called_once()


def test_mixed_data_loader():
    spark = MagicMock()
    table_config = {
        "table1": {
            "load_date_column": "load_date",
            "catalogue_schema": "schema1",
            "kpi_keys": ["kpi1"],
            "match_keys": ["colA"],
        }
    }
    tables = ["table1"]
    table_prefix = "prefix_"
    filter_date = "2023-01-01"
    overwrite_date = "2023-01-02"

    dummy_df = MagicMock()
    dummy_df.columns = ["col1", "load_date", "kpi1", "col3"]
    dummy_df.withColumn.return_value = dummy_df
    dummy_df.select.return_value = dummy_df
    dummy_df.drop.return_value = dummy_df

    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    spark.table.return_value = dummy_df
    spark.sql.return_value = dummy_df

    with patch("funcs.general_functions.f", _patched_fake_f()), patch("funcs.general_functions.w", fake_w):
        mixed_data_loader(
            spark, table_config, tables, table_prefix, filter_date, overwrite_date
        )

    spark.sql.assert_called_once()
    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")


def test_add_columns():
    spark = MagicMock()
    table_config = {"table1": {"load_date_column": "load_date"}}
    tables = ["table1"]
    table_prefix = "prefix_"
    filter_date = "2023-01-01"
    overwrite_date = "2023-01-02"

    dummy_df = MagicMock()
    dummy_df.withColumn.return_value = dummy_df
    dummy_df.select.return_value = dummy_df

    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    spark.sql.side_effect = [None, dummy_df]

    add_columns(spark, table_config, tables, table_prefix, filter_date, overwrite_date)

    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[0][0][0]
    select_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE" in alter_call
    assert "SELECT * FROM" in select_call

    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")


def test_drop_columns():
    spark = MagicMock()
    table_config = {"table1": {"load_date_column": "load_date"}}
    tables = ["table1"]
    table_prefix = "prefix_"
    filter_date = "2023-01-01"
    overwrite_date = "2023-01-02"

    dummy_df = MagicMock()
    dummy_df.withColumn.return_value = dummy_df
    dummy_df.select.return_value = dummy_df

    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    spark.sql.side_effect = [None, dummy_df]

    drop_columns(spark, table_config, tables, table_prefix, filter_date, overwrite_date)

    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[0][0][0]
    select_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE" in alter_call and "DROP COLUMN" in alter_call
    assert "SELECT * FROM" in select_call

    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")