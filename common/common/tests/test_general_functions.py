# Databricks notebook source
import sys, os

# Set up repo path
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)


import pyspark.sql.functions as f
from unittest.mock import patch, MagicMock, call
from collections import namedtuple
from functools import wraps
import types
from io import StringIO
from collections import namedtuple
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from funcs.general_functions import *


def suppress_print(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)

    return wrapper

# COMMAND ----------

# --- Test for init_empty_tables ---
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

    # Call the function
    init_empty_tables(spark, table_config, tables, table_prefix)

    # Each table should trigger 3 spark.sql calls (drop, create, grant)
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
    return f"test_init_empty_tables: PASSED"
    

@suppress_print
def test_init_empty_tables_single_success():
    '''
    This test ensures that:
        • All three SQL statements (DROP, CREATE … LIKE, GRANT) are issued,
          in that order, for a single table.
        • Each statement contains the fully-qualified object name constructed
          from `table_prefix` + table.
        • No exception bubbles up to the caller.
    '''
    spark = MagicMock()
    spark.sql.return_value = MagicMock()           # dummy DF return
    table_config = {"orders": {"catalogue_schema": "cat_db"}}
    tables = ["orders"]
    prefix = "tgt_"

    init_empty_tables(spark, table_config, tables, prefix)

    drop_q, create_q, grant_q = (c[0][0].strip() for c in spark.sql.call_args_list)

    assert drop_q.startswith("DROP TABLE IF EXISTS tgt_orders")
    assert "CREATE TABLE tgt_orders" in create_q and "LIKE cat_db.orders" in create_q
    assert "GRANT MANAGE ON tgt_orders" in grant_q
    assert spark.sql.call_count == 3, "Expected three SQL calls"
    return "test_init_empty_tables_single_success: PASSED"


@suppress_print
def test_init_empty_tables_multi_success():
    '''
    This test ensures that:
        • The function loops over each element in `tables`.
        • Exactly three SQL calls per table are executed (DROP, CREATE, GRANT),
          resulting in 3 × N total calls.
        • Table-specific fragments appear in the generated SQL text.
    '''
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
    # Verify each table appears in its respective CREATE statement
    create_statements = [c[0][0] for c in spark.sql.call_args_list if "CREATE TABLE" in c[0][0]]
    assert any("p_t1" in stmt for stmt in create_statements)
    assert any("p_t2" in stmt for stmt in create_statements)
    return "test_init_empty_tables_multi_success: PASSED"


@suppress_print
def test_init_empty_tables_drop_failure():
    '''
    This test ensures that:
        • If the initial DROP statement fails, the exception is caught, the
          loop continues (or ends) without raising, and no CREATE/GRANT is run
          for that table.
        • Only one call to `spark.sql` is made when DROP raises.
    '''
    spark = MagicMock()
    spark.sql.side_effect = Exception("drop failed")   # fail on first call
    table_config = {"bad": {"catalogue_schema": "cat"}}
    tables = ["bad"]

    init_empty_tables(spark, table_config, tables, "x_")

    assert spark.sql.call_count == 1, "CREATE/GRANT should be skipped after drop failure"
    return "test_init_empty_tables_drop_failure: PASSED"


@suppress_print
def test_init_empty_tables_create_failure():
    '''
    This test ensures that:
        • A failure on the CREATE … LIKE statement is caught.
        • The GRANT statement is **not** executed after the failure.
        • Exactly two SQL calls occur (DROP succeeds, CREATE raises).
    '''
    spark = MagicMock()
    spark.sql.side_effect = [None, Exception("create failed")]   # drop ok, create boom
    table_config = {"t": {"catalogue_schema": "cat"}}

    init_empty_tables(spark, table_config, ["t"], "x_")

    assert spark.sql.call_count == 2, "GRANT should not be attempted on create failure"
    return "test_init_empty_tables_create_failure: PASSED"


@suppress_print
def test_init_empty_tables_missing_config():
    '''
    This test ensures that:
        • When a table is absent from `table_config`, a `KeyError` triggered
          during CREATE-statement composition is caught by the outer try/except.
        • Only the DROP statement is executed; subsequent statements are
          skipped because the error happens before the CREATE call.
    '''
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    table_config = {}               # empty ⇒ table lookup fails
    tables = ["ghost"]

    init_empty_tables(spark, table_config, tables, "p_")

    assert spark.sql.call_count == 1, "Only DROP should run when config is missing"
    drop_stmt = spark.sql.call_args_list[0][0][0]
    assert "DROP TABLE IF EXISTS p_ghost" in drop_stmt
    return "test_init_empty_tables_missing_config: PASSED"

# COMMAND ----------

@suppress_print
def test_data_loader_single_day_success():
    '''
    This test ensures that **single_day** mode:
        • Selects the "=" operator in the date filter clause.
        • Builds a multi-column join condition in the order provided by
          `match_keys`.
        • Invokes `spark.sql` exactly once with a well-formed MERGE statement.
    '''
    from __main__ import data_loader

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

    # ── Assertions
    spark.sql.assert_called_once()
    sql_text = spark.sql.call_args[0][0]
    assert "MERGE INTO tgt_orders" in sql_text
    assert "cat.orders" in sql_text
    assert "CAST(load_ts as timestamp) = '2025-06-30'" in sql_text
    assert "tgt.id = src.id AND tgt.region = src.region" in sql_text
    return "test_data_loader_single_day_success: PASSED"


@suppress_print
def test_data_loader_multi_day_two_tables():
    '''
    This test ensures that **multi_day** mode:
        • Uses the ">=" operator.
        • Iterates over *each* table in `tables`, issuing one SQL call per item
          (thus 2 calls total in this case).
        • Correctly swaps in each table’s schema & keys.
    '''
    from __main__ import data_loader

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
    # Check operator once; both will contain it.
    for call_ in spark.sql.call_args_list:
        assert ">='2025-01-01'".replace("'>", "' >")[:-1] in call_[0][0].replace(" ", ""), (
            "Operator >= expected in filter"
        )
    return "test_data_loader_multi_day_two_tables: PASSED"


@suppress_print
def test_data_loader_invalid_mode():
    '''
    This test ensures that:
        • A mode other than "single_day"/"multi_day" prints a warning and
          returns **without** calling `spark.sql`.
    '''
    from __main__ import data_loader

    spark = MagicMock()
    data_loader(
        spark,
        table_config={},
        tables=[],
        table_prefix="p_",
        filter_date="2025-01-01",
        mode="yesterday",             # invalid
    )

    spark.sql.assert_not_called()
    return "test_data_loader_invalid_mode: PASSED"


@suppress_print
def test_data_loader_table_not_in_config():
    '''
    This test ensures that:
        • If a table listed in `tables` is absent from `table_config`, a
          `KeyError` is raised, propagating to the caller (no silent swallow).
    '''
    from __main__ import data_loader

    spark = MagicMock()
    try:
        data_loader(
            spark,
            table_config={},           # empty → no entry for "ghost"
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-01-01",
        )
        assert False, "Expected KeyError for missing table_config entry"
    except KeyError:
        pass
    return "test_data_loader_table_not_in_config: PASSED"


@suppress_print
def test_data_loader_missing_match_keys():
    '''
    This test ensures that:
        • When `match_keys` is not supplied in a table’s config, the join-string
          comprehension raises a `KeyError`, which propagates outward.
    '''
    from __main__ import data_loader

    spark = MagicMock()
    cfg = {
        "err_tbl": {
            "catalogue_schema": "c",
            "load_date_column": "ld",
            # "match_keys" intentionally omitted
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
    return "test_data_loader_missing_match_keys: PASSED"


@suppress_print
def test_data_loader_empty_tables():
    '''
    This test ensures that:
        • Passing an empty `tables` list results in **zero** SQL calls.
        • Function exits cleanly.
    '''
    from __main__ import data_loader

    spark = MagicMock()
    data_loader(
        spark,
        table_config={},
        tables=[],
        table_prefix="p_",
        filter_date="2025-01-01",
    )

    spark.sql.assert_not_called()
    return "test_data_loader_empty_tables: PASSED"

# COMMAND ----------

@suppress_print
def test_duplicate_data_loader_append_single_success():
    '''
    This test ensures that:
        • `overwrite_date` ≠ `filter_date`, so the function proceeds.
        • A MERGE-style workflow is emulated:
            - `spark.table` is called once to fetch column order.
            - `spark.sql` is called once to pull the rows to duplicate.
            - `withColumn`, `select`, and `write.mode("append").saveAsTable` are chained in that exact sequence.
        • The final `saveAsTable` targets the fully-qualified name
          `{table_prefix}{table}`.
    '''
    # ── Spark & DataFrame mocks
    spark = MagicMock()
    df_mock = MagicMock(name="DataFrame")
    df_mock.columns = ["c1", "c2", "dt_col"]

    # chain: withColumn -> select -> df_write
    df_write = MagicMock(name="WriteObj")
    df_write_mode = MagicMock(name="ModeObj")
    df_write.mode.return_value = df_write_mode
    df_write_mode.saveAsTable.return_value = None
    df_mock.write = df_write
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    # fake `f` namespace (to_timestamp & lit)
    fake_f = types.SimpleNamespace(
        to_timestamp=lambda x: x,
        lit=lambda x: x,
    )

    cfg = {"orders": {"load_date_column": "dt_col"}}

    with patch("__main__.f", fake_f):
        duplicate_data_loader_append(
            spark,
            table_config=cfg,
            tables=["orders"],
            table_prefix="tgt_",
            filter_date="2025-06-30",
            overwrite_date="2025-07-01",
        )

    # ── Assertions
    spark.table.assert_called_once_with("tgt_orders")
    spark.sql.assert_called_once()
    df_mock.withColumn.assert_called_once()          # overwrite date col
    df_mock.select.assert_called_once_with(*df_mock.columns)
    df_write.mode.assert_called_once_with("append")
    df_write_mode.saveAsTable.assert_called_once_with("tgt_orders")
    return "test_duplicate_data_loader_append_single_success: PASSED"


@suppress_print
def test_duplicate_data_loader_append_same_dates_early_exit():
    '''
    This test ensures that:
        • When `overwrite_date == filter_date`, the function prints a warning
          and returns immediately.
        • No Spark interactions occur.
    '''
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
    return "test_duplicate_data_loader_append_same_dates_early_exit: PASSED"


@suppress_print
def test_duplicate_data_loader_append_multi_tables():
    '''
    This test ensures that:
        • The function iterates over every table in `tables`.
        • For N tables, `spark.table` and `spark.sql` are each called N times,
          and `saveAsTable` is invoked N times.
    '''
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

    fake_f = types.SimpleNamespace(to_timestamp=lambda x: x, lit=lambda x: x)

    cfg = {
        "t1": {"load_date_column": "dt"},
        "t2": {"load_date_column": "dt"},
    }

    with patch("__main__.f", fake_f):
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
    return "test_duplicate_data_loader_append_multi_tables: PASSED"


@suppress_print
def test_duplicate_data_loader_append_missing_config():
    '''
    This test ensures that:
        • When a table is absent from `table_config`, the attempted dictionary
          lookup raises a `KeyError` that propagates to the caller.
        • No write/append occurs after the error.
    '''
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])

    try:
        duplicate_data_loader_append(
            spark,
            table_config={},            # empty config
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )
        assert False, "Expected KeyError due to missing config entry"
    except KeyError:
        pass
    return "test_duplicate_data_loader_append_missing_config: PASSED"


@suppress_print
def test_duplicate_data_loader_append_sql_failure():
    '''
    This test ensures that:
        • If `spark.sql` raises (e.g., catalog outage), the exception propagates
          and **write** is never attempted.
    '''
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("boom")

    cfg = {"tbl": {"load_date_column": "d"}}

    fake_f = types.SimpleNamespace(to_timestamp=lambda x: x, lit=lambda x: x)

    try:
        with patch("__main__.f", fake_f):
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
    return "test_duplicate_data_loader_append_sql_failure: PASSED"


def test_duplicate_data_loader_append_different_dates():
    spark = MagicMock()
    table_config = {"table1": {"load_date_column": "load_date"}}
    tables = ["table1"]
    table_prefix = "prefix_"
    filter_date = "2023-01-01"
    overwrite_date = "2023-01-02"

    # Setup a dummy DataFrame for spark.table with a columns attribute.
    dummy_df = MagicMock()
    dummy_df.columns = ["col1", "load_date", "col3"]
    # Ensure that the chained methods return the same dummy_df.
    dummy_df.withColumn.return_value = dummy_df
    dummy_df.select.return_value = dummy_df
    dummy_df.write.mode.return_value = dummy_df

    # Ensure spark.table and spark.sql return our dummy_df.
    spark.table.return_value = dummy_df
    spark.sql.return_value = dummy_df

    duplicate_data_loader_append(
        spark, table_config, tables, table_prefix, filter_date, overwrite_date
    )

    # Check that spark.sql was called with a SELECT statement.
    spark.sql.assert_called_once()
    call_sql = spark.sql.call_args[0][0]
    assert "SELECT *" in call_sql

    # Now, the chained call to 'saveAsTable' should be recorded on our dummy_df.
    dummy_df.write.mode("append").saveAsTable.assert_called_once_with(
        f"{table_prefix}table1"
    )
    return f"test_duplicate_data_loader_append_different_dates: PASSED"

# COMMAND ----------

def _fake_column(name: str) -> MagicMock:
    col_mock = MagicMock(name=f"col({name})")
    col_mock._expr = f"<expr for {name}>"      # satisfies Spark internals
    col_mock.isNull.return_value = MagicMock(name=f"isNull({name})", _expr="<isNull>")
    return col_mock

# A reusable fake-f namespace
import types
fake_f = types.SimpleNamespace(
    to_timestamp=lambda x: MagicMock(name="to_timestamp", _expr="<to_ts>"),
    lit=lambda x: MagicMock(name=f"lit({x})", _expr=f"<lit {x}>"),
    when=lambda cond, val: MagicMock(name="when()", _expr="<when>"),
    col=_fake_column,
)


@suppress_print
def test_new_data_loader_kpis_success():
    '''
    This test ensures that:
        • overwrite_date ≠ filter_date → loader proceeds.
        • The KPI branch fires (`type == "kpis"`), so `withColumn` is called
          once per KPI column with a *Column* expression (no AttributeError).
        • Column order is preserved and data is written in append mode.
    '''
    spark = MagicMock()

    # Mock DataFrame and its chaining behaviour
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

    with patch("__main__.f", fake_f):
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
    # 1 date-overwrite + 1 KPI column  ==> 2 calls
    assert df_mock.withColumn.call_count == 2
    df_mock.write.mode.assert_called_once_with("append")
    df_mock.write.mode.return_value.saveAsTable.assert_called_once_with("tgt_orders")
    return "test_new_data_loader_kpis_success: PASSED"


@suppress_print
def test_new_data_loader_non_kpis_success():
    '''
    Asserts the **else** branch: string substitution instead of KPI math.
    '''
    from __main__ import new_data_loader

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

    with patch("__main__.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["tbl"],
            table_prefix="p_",
            filter_date="2025-05-01",
            overwrite_date="2025-05-02",
        )

    spark.table.assert_called_once_with("p_tbl")
    # 1 date overwrite + 1 KPI string-branch  ==> 2 calls
    assert df_mock.withColumn.call_count == 2
    df_mock.write.mode.assert_called_once_with("append")
    return "test_new_data_loader_non_kpis_success: PASSED"


@suppress_print
def test_new_data_loader_same_dates():
    '''
    This test ensures that:
        • When `overwrite_date == filter_date`, the loader prints a warning and
          returns immediately.
        • No Spark calls are made.
    '''
    from __main__ import new_data_loader

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
    return "test_new_data_loader_same_dates: PASSED"


@suppress_print
def test_new_data_loader_multi_tables():
    '''
    This test ensures that:
        • The function loops over each table in `tables`.
        • For N tables, `spark.table`, `spark.sql`, and `saveAsTable` are each
          invoked N times.
    '''
    from __main__ import new_data_loader

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

    fake_f = types.SimpleNamespace(
        to_timestamp=lambda x: x,
        lit=lambda x: x,
        when=lambda cond, val: None,
        col=lambda name: None,
    )

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

    with patch("__main__.f", fake_f):
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
    return "test_new_data_loader_multi_tables: PASSED"


@suppress_print
def test_new_data_loader_missing_config():
    '''
    This test ensures that:
        • A table absent from `table_config` raises a `KeyError` which propagates
          to the caller.
    '''
    from __main__ import new_data_loader

    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c"])

    try:
        new_data_loader(
            spark,
            table_config={},            # empty
            tables=["ghost"],
            table_prefix="p_",
            filter_date="2025-06-01",
            overwrite_date="2025-07-01",
        )
        assert False, "Expected KeyError for missing config"
    except KeyError:
        pass
    return "test_new_data_loader_missing_config: PASSED"


@suppress_print
def test_new_data_loader_sql_failure():
    '''
    This test ensures that:
        • If `spark.sql` raises, the exception bubbles up and **write** methods
          are never called.
    '''
    from __main__ import new_data_loader

    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("SQL failure")

    cfg = {"tbl": {"load_date_column": "d", "type": "kpis", "kpi_keys": []}}

    fake_f = types.SimpleNamespace(to_timestamp=lambda x: x, lit=lambda x: x)

    try:
        with patch("__main__.f", fake_f):
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
    return "test_new_data_loader_sql_failure: PASSED"


@suppress_print
def test_new_data_loader_empty_kpi_list():
    '''
    This test ensures that:
        • When `kpi_keys` is an empty list, no additional `withColumn` calls
          are performed beyond the date overwrite.
    '''
    from __main__ import new_data_loader

    spark = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["id", "dt"]
    df_mock.withColumn.return_value = df_mock
    df_mock.select.return_value = df_mock
    df_mock.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df_mock
    spark.sql.return_value = df_mock

    fake_f = types.SimpleNamespace(
        to_timestamp=lambda x: x,
        lit=lambda x: x,
        when=lambda *args, **kwargs: None,
        col=lambda name: None,
    )

    cfg = {
        "tbl": {
            "load_date_column": "dt",
            "type": "kpis",
            "kpi_keys": [],    # empty
        }
    }

    with patch("__main__.f", fake_f):
        new_data_loader(
            spark,
            table_config=cfg,
            tables=["tbl"],
            table_prefix="p_",
            filter_date="2025-01-01",
            overwrite_date="2025-02-01",
        )

    # Date overwrite + NO KPI cols => exactly 1 withColumn
    assert df_mock.withColumn.call_count == 1
    return "test_new_data_loader_empty_kpi_list: PASSED"

# COMMAND ----------

# ------------------------------------------------------------------
# Utility: extend the *existing* fake_f with a row_number stub
# ------------------------------------------------------------------
def _patched_fake_f():
    """
    Returns the previously-defined `fake_f` namespace but with an extra
    `row_number` attribute (a Column-like mock exposing .over() & _expr).
    """
    # only patch once
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
#  I1  ─ Single table, one KPI (happy path)
# =============================================================================
@suppress_print
def test_mixed_data_loader_single_kpi_success():
    '''
    Verifies happy-path behaviour for one table with one KPI key:
      • overwrite_date ≠ filter_date → processing proceeds.
      • withColumn is invoked three times
          1) overwrite load_date
          2) add row_number
          3) transform KPI column
      • drop("row_number") is called.
      • Data is written in append mode to the expected table.
    '''
    spark = MagicMock()

    # --- DataFrame mock with chaining semantics
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

    with patch("__main__.f", _patched_fake_f()), patch("__main__.w", fake_w):
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
    return "test_mixed_data_loader_single_kpi_success: PASSED"


# =============================================================================
#  I2  ─ Early exit when dates are identical
# =============================================================================
@suppress_print
def test_mixed_data_loader_same_dates_exit():
    '''
    Ensures no Spark interactions when overwrite_date == filter_date.
    '''
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
    return "test_mixed_data_loader_same_dates_exit: PASSED"


# =============================================================================
#  I3  ─ Two tables, two KPI keys each
# =============================================================================
@suppress_print
def test_mixed_data_loader_multi_tables_multi_kpis():
    '''
    Loop-coverage test:
      • `spark.table/sql` invoked for each table (2×).
      • withColumn count equals N × (1 + 2×kpi_keys).
      • saveAsTable called for each table.
    '''
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

    with patch("__main__.f", _patched_fake_f()), patch("__main__.w", fake_w):
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
    return "test_mixed_data_loader_multi_tables_multi_kpis: PASSED"


# =============================================================================
#  I4  ─ Missing table config raises KeyError
# =============================================================================
@suppress_print
def test_mixed_data_loader_missing_config():
    '''
    Ensures KeyError propagates when a table is absent from `table_config`.
    '''
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
    return "test_mixed_data_loader_missing_config: PASSED"


# =============================================================================
#  I5  ─ spark.sql failure propagates
# =============================================================================
@suppress_print
def test_mixed_data_loader_sql_failure():
    '''
    Ensures exceptions from spark.sql bubble up and prevent writes.
    '''
    spark = MagicMock()
    spark.table.return_value = MagicMock(columns=["c", "d"])
    spark.sql.side_effect = Exception("sql boom")

    cfg = {"tbl": {"load_date_column": "d", "kpi_keys": []}}

    with patch("__main__.f", _patched_fake_f()), patch("__main__.w", fake_w):
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
    return "test_mixed_data_loader_sql_failure: PASSED"


# =============================================================================
#  I6  ─ Empty KPI list results in single withColumn
# =============================================================================
@suppress_print
def test_mixed_data_loader_empty_kpi_list():
    '''
    With no KPI keys, loader should only perform the load_date overwrite
    (i.e., exactly one withColumn call).
    '''
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "dt"]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.write.mode.return_value.saveAsTable.return_value = None

    spark.table.return_value = df
    spark.sql.return_value = df

    cfg = {"tbl": {"load_date_column": "dt", "kpi_keys": []}}

    with patch("__main__.f", _patched_fake_f()), patch("__main__.w", fake_w):
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
    return "test_mixed_data_loader_empty_kpi_list: PASSED"

# COMMAND ----------

# --- Test for mixed_data_loader ---
def test_mixed_data_loader():
    from pyspark.sql import Window  # Import the actual Window class.

    global w
    w = Window  # Make it available as the variable 'w' expected by mixed_data_loader.

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
    dummy_df.drop.return_value = (
        dummy_df  # Ensure that drop() returns the same dummy_df.
    )

    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    spark.table.return_value = dummy_df
    spark.sql.return_value = dummy_df

    mixed_data_loader(
        spark, table_config, tables, table_prefix, filter_date, overwrite_date
    )

    spark.sql.assert_called_once()
    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")
    return "Test passed"


# --- Test for add_columns ---
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

    # Create a mode mock for the chained call.
    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    # Setup side_effect: first call for ALTER TABLE returns None, second call for the SELECT query returns dummy_df.
    spark.sql.side_effect = [None, dummy_df]

    add_columns(spark, table_config, tables, table_prefix, filter_date, overwrite_date)

    # Two SQL calls should have been made.
    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[0][0][0]
    select_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE" in alter_call
    assert "SELECT * FROM" in select_call

    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")
    return f"Test passed"


# --- Test for drop_columns ---
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

    # Create a mode mock for the chained call.
    mode_mock = MagicMock()
    dummy_df.write.mode.return_value = mode_mock

    # Setup side_effect: first call for ALTER TABLE, second for the SELECT query.
    spark.sql.side_effect = [None, dummy_df]

    drop_columns(spark, table_config, tables, table_prefix, filter_date, overwrite_date)

    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[0][0][0]
    select_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE" in alter_call and "DROP COLUMN" in alter_call
    assert "SELECT * FROM" in select_call

    mode_mock.saveAsTable.assert_called_once_with(f"{table_prefix}table1")
    return f"Test passed"

# COMMAND ----------

if __name__ == "__main__":
    print(test_init_empty_tables())
    print(test_init_empty_tables_single_success())
    print(test_init_empty_tables_multi_success())
    print(test_init_empty_tables_drop_failure())
    print(test_init_empty_tables_create_failure())
    print(test_init_empty_tables_missing_config())

    print(test_data_loader_single_day_success())
    print(test_data_loader_multi_day_two_tables())
    print(test_data_loader_invalid_mode())
    print(test_data_loader_table_not_in_config())
    print(test_data_loader_missing_match_keys())
    print(test_data_loader_empty_tables())

    print(test_duplicate_data_loader_append_single_success())
    print(test_duplicate_data_loader_append_same_dates_early_exit())
    print(test_duplicate_data_loader_append_multi_tables())
    print(test_duplicate_data_loader_append_missing_config())
    print(test_duplicate_data_loader_append_sql_failure())
    print(test_duplicate_data_loader_append_different_dates())

    print(test_new_data_loader_kpis_success())
    print(test_new_data_loader_non_kpis_success())
    print(test_new_data_loader_same_dates())
    print(test_new_data_loader_multi_tables())
    print(test_new_data_loader_missing_config())
    print(test_new_data_loader_sql_failure())
    print(test_new_data_loader_empty_kpi_list())

    print(test_mixed_data_loader_single_kpi_success())
    print(test_mixed_data_loader_same_dates_exit())
    print(test_mixed_data_loader_multi_tables_multi_kpis())
    print(test_mixed_data_loader_missing_config())
    print(test_mixed_data_loader_sql_failure())
    print(test_mixed_data_loader_empty_kpi_list())

    print(test_add_columns())

    print(test_drop_columns())

    print(f"\n✅ All tests passed.")

# COMMAND ----------

