# Databricks notebook source
# MAGIC %pip install pytest
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# %pip install pytest
# dbutils.library.restartPython()
import os
import pytest
import sys
# import json
from unittest.mock import patch, MagicMock, ANY, call
from functools import wraps

# Set up repo path
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from funcs.utils import (
    # General Utility Helpers
    ensure_dict_key_prefix,
    remove_all_angle_brackets,
    
    # SQL Formatting Helpers (including the protected ones!)
    _sql_lit,
    _quote_3part,
    
    # Catalog & Schema Metadata
    table_exists,
    get_table_schema,
    generate_table_list,
    generate_table_key_data,
    generate_fk_script_df,
    
    # Delta Write & Merge Operations
    overwrite_to_delta,
    append_to_delta,
    unify_tables,
    merge_into_delta
)
# ---------------- helpers ----------------

def suppress_print(func):
    """Decorator to suppress print statements during function execution."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)
    return wrapper

class _MockField:
    """Mimic a Spark StructField's dataType.simpleString()."""
    def __init__(self, simple: str):
        self.dataType = MagicMock()
        self.dataType.simpleString = MagicMock(return_value=simple)


# ===================== GENERAL UTILITY HELPERS =====================

def test_ensure_dict_key_prefix():
    # 1. Empty prefix should return a copy of the dictionary
    original = {"table_a": 1}
    assert ensure_dict_key_prefix(original, "") == original

    # 2. Key already has the exact prefix
    prefix = "dev_gold_model_"
    assert ensure_dict_key_prefix({"dev_gold_model_table": 1}, prefix) == {"dev_gold_model_table": 1}

    # 3. Smart overlap detection (partial prefix already there)
    assert ensure_dict_key_prefix({"gold_model_aum_bridge": 1}, prefix) == {"dev_gold_model_aum_bridge": 1}
    assert ensure_dict_key_prefix({"model_aum_bridge": 1}, prefix) == {"dev_gold_model_aum_bridge": 1}

    # 4. No overlap (prefix prepended fully)
    assert ensure_dict_key_prefix({"aum_bridge": 1}, prefix) == {"dev_gold_model_aum_bridge": 1}


def test_remove_all_angle_brackets():
    # 1. Simple array
    assert remove_all_angle_brackets("array<string>") == "array"
    
    # 2. Nested map/array
    assert remove_all_angle_brackets("map<string, array<int>>") == "map"
    
    # 3. No brackets (should remain unchanged)
    assert remove_all_angle_brackets("string") == "string"


# ===================== SQL FORMATTING HELPERS =====================

def test_sql_lit():
    assert _sql_lit(None) == "NULL"
    assert _sql_lit(123) == "123"
    assert _sql_lit(12.34) == "12.34"
    assert _sql_lit("hello") == "'hello'"
    # Test SQL injection/quote escaping
    assert _sql_lit("O'Connor") == "'O''Connor'"


def test_quote_3part():
    # 1. Happy path (automatically adds backticks)
    assert _quote_3part("cat.schema.tbl") == "`cat`.`schema`.`tbl`"
    # 2. Ignores existing backticks and fixes them
    assert _quote_3part("`cat`.schema.`tbl`") == "`cat`.`schema`.`tbl`"
    
    # 3. Raises exception on invalid formats
    with pytest.raises(Exception) as exc:
        _quote_3part("schema.tbl")
    assert "target must be 3-part" in str(exc.value)


# ===================== CATALOG & SCHEMA METADATA =====================

def test_table_exists():
    spark = MagicMock()
    
    # 1-part name
    spark.catalog.tableExists.return_value = True
    assert table_exists(spark, "my_table") is True
    spark.catalog.tableExists.assert_called_with("my_table")
    
    # 2-part name
    spark.catalog.tableExists.return_value = False
    assert table_exists(spark, "my_db.my_table") is False
    spark.catalog.tableExists.assert_called_with("my_db", "my_table")
    
    # 3-part name (Unity Catalog)
    mock_df = MagicMock()
    mock_df.count.return_value = 1
    spark.sql.return_value = mock_df
    assert table_exists(spark, "my_cat.my_schema.my_table") is True
    
    called_sql = str(spark.sql.call_args[0][0])
    assert "`my_cat`.information_schema.tables" in called_sql
    assert "table_schema = 'my_schema'" in called_sql
    assert "table_name = 'my_table'" in called_sql


def test_get_table_schema():
    spark = MagicMock()
    mock_df = MagicMock()
    spark.sql.return_value = mock_df
    
    # Mock the dataframe chaining
    mock_filtered = MagicMock()
    mock_df.filter.return_value = mock_filtered
    mock_selected = MagicMock()
    mock_filtered.select.return_value = mock_selected
    
    result = get_table_schema(spark, "cat.schema", "my_table")
    
    spark.sql.assert_called_with("DESCRIBE TABLE EXTENDED cat.schema.my_table")
    mock_filtered.select.assert_called_with("col_name", "data_type", "comment")
    assert result == mock_selected

    # Test Exception handling
    spark.sql.side_effect = Exception("Not found")
    with pytest.raises(Exception) as exc:
        get_table_schema(spark, "cat.schema", "my_table")
    assert "Error getting table schema for cat.schema.my_table: Not found" in str(exc.value)


def test_generate_table_list():
    spark = MagicMock()
    mock_df = MagicMock()
    spark.sql.return_value = mock_df
    
    # Mocking the filter -> select -> collect chain
    mock_filtered = MagicMock()
    mock_df.filter.return_value = mock_filtered
    mock_selected = MagicMock()
    mock_filtered.select.return_value = mock_selected
    mock_selected.collect.return_value = [("table_a",), ("table_b",)]
    
    result = generate_table_list(spark, "my_cat", "my_schema", "MANAGED")
    
    called_sql = str(spark.sql.call_args[0][0])
    assert "my_cat.information_schema.tables" in called_sql
    mock_filtered.select.assert_called_with("table_name")
    assert result == ["table_a", "table_b"]


def test_generate_table_key_data():
    spark = MagicMock()
    mock_df = MagicMock()
    spark.sql.return_value = mock_df
    
    # Chain mocks for DataFrame transformations to prevent crash
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.union.return_value = mock_df
    mock_df.select.return_value = mock_df
    
    # 1. Empty tables list
    assert generate_table_key_data(spark, [], "my_schema") is None
    
    # 2. Populated tables list
    result = generate_table_key_data(spark, ["tbl1", "tbl2"], "my_schema")
    
    spark.sql.assert_any_call("DESCRIBE TABLE EXTENDED my_schema.tbl1")
    spark.sql.assert_any_call("DESCRIBE TABLE EXTENDED my_schema.tbl2")
    assert result == mock_df # Returns the final chained mock


def test_generate_fk_script_df():
    # Setup mock dataframe
    mock_df = MagicMock()
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    
    result = generate_fk_script_df(mock_df, "my_schema")
    
    # Ensure dataframe transformations were called
    mock_df.filter.assert_called()
    mock_df.select.assert_called()
    assert mock_df.withColumn.call_count == 4  # fk_col, constraint, fk_name, script
    assert result == mock_df


# ===================== overwrite_to_delta / append_to_delta =====================

@suppress_print
def test_overwrite_to_delta_writes_with_overwriteSchema():
    spark = MagicMock()
    df = MagicMock()

    overwrite_to_delta(spark, df, "main.default.test_table")

    df.write.format.assert_called_with("delta")
    df.write.format.return_value.mode.assert_called_with("overwrite")
    df.write.format.return_value.mode.return_value.option.assert_called_with("overwriteSchema", "true")
    df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_with(
        "main.default.test_table"
    )

@suppress_print
def test_append_to_delta_writes_with_mergeSchema():
    spark = MagicMock()
    df = MagicMock()

    append_to_delta(spark, df, "main.default.test_table")

    df.write.format.assert_called_with("delta")
    df.write.format.return_value.mode.assert_called_with("append")
    df.write.format.return_value.mode.return_value.option.assert_called_with("mergeSchema", "true")
    df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_with(
        "main.default.test_table"
    )


# ===================== merge_into_delta: validation & 3-part target =====================

@suppress_print
def test_merge_into_delta_raises_on_invalid_target_not_3part():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]

    with pytest.raises(Exception) as exc:
        merge_into_delta(spark, df, "default.test_table", match_keys=["id"])
    assert "3-part" in str(exc.value)

@suppress_print
def test_merge_into_delta_raises_on_empty_match_keys():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]

    with pytest.raises(Exception) as exc:
        merge_into_delta(spark, df, "main.default.test_table", match_keys=[])
    assert "match_keys must be a non-empty list" in str(exc.value)

@suppress_print
def test_merge_into_delta_raises_when_match_key_missing_in_df():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]

    with pytest.raises(Exception) as exc:
        merge_into_delta(spark, df, "main.default.test_table", match_keys=["missing"])
    assert "missing from DataFrame" in str(exc.value)


# ===================== merge_into_delta: table creation path =====================

@suppress_print
def test_merge_into_delta_creates_table_when_not_exists():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]
    df.createOrReplaceTempView = MagicMock()

    with patch("funcs.utils.table_exists", return_value=False):
        spark.table.return_value.columns = ["id", "v"]

        merge_into_delta(spark, df, "main.default.test_table", match_keys=["id"])

        sql_calls = " \n ".join(str(call.args[0]) for call in spark.sql.call_args_list)
        assert "CREATE TABLE IF NOT EXISTS `main`.`default`.`test_table`" in sql_calls
        assert "MERGE INTO `main`.`default`.`test_table`" in sql_calls


# ===================== merge_into_delta: scoped/full delete behavior =====================

@suppress_print
def test_merge_into_delta_scoped_delete_requires_period_col():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]

    with pytest.raises(Exception) as exc:
        merge_into_delta(
            spark, df, "main.default.test_table",
            match_keys=["id"],
            delete_mode="scoped",
            period_column=None
        )
    assert "period_column is required" in str(exc.value)

@suppress_print
def test_merge_into_delta_scoped_delete_executes_delete_with_scope_values_from_df():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v", "period"]
    df.createOrReplaceTempView = MagicMock()
    sel = MagicMock()
    df.select.return_value = sel
    sel.distinct.return_value.collect.return_value = [("2025-08-01",), ("2025-08-02",)]

    with patch("funcs.utils.table_exists", return_value=True):
        spark.table.return_value.columns = ["id", "v", "period"]

        merge_into_delta(
            spark, df, "main.default.test_table",
            match_keys=["id"],
            delete_mode="scoped",
            period_column="period"
        )

        sql_calls = " \n ".join(str(call.args[0]) for call in spark.sql.call_args_list)
        assert "DELETE FROM `main`.`default`.`test_table` AS tgt" in sql_calls
        assert "tgt.`period` IN ('2025-08-01', '2025-08-02')" in sql_calls
        assert "MERGE INTO `main`.`default`.`test_table`" in sql_calls

@suppress_print
def test_merge_into_delta_full_delete_with_filter_executes_delete():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v"]
    df.createOrReplaceTempView = MagicMock()

    with patch("funcs.utils.table_exists", return_value=True):
        spark.table.return_value.columns = ["id", "v"]

        merge_into_delta(
            spark, df, "main.default.test_table",
            match_keys=["id"],
            delete_mode="full",
            delete_filter="tgt.load_dt >= '2025-09-01'"
        )

        sql_calls = " \n ".join(str(call.args[0]) for call in spark.sql.call_args_list)
        assert "DELETE FROM `main`.`default`.`test_table` AS tgt" in sql_calls
        assert "tgt.load_dt >= '2025-09-01'" in sql_calls
        assert "MERGE INTO `main`.`default`.`test_table`" in sql_calls


# ===================== merge_into_delta: schema padding & evolution =====================

@suppress_print
def test_merge_into_delta_pads_missing_source_columns_with_nulls():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id"]
    df.withColumn.return_value = df
    df.createOrReplaceTempView = MagicMock()

    with patch("funcs.utils.table_exists", return_value=True):
        spark.table.return_value.columns = ["id", "v"]

        merge_into_delta(spark, df, "main.default.test_table", match_keys=["id"])

        df.withColumn.assert_any_call("v", ANY)
        assert spark.sql.call_count > 0

@suppress_print
def test_merge_into_delta_adds_new_columns_with_inferred_types():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "v", "extra"]
    df.createOrReplaceTempView = MagicMock()
    df.schema = {
        "id": _MockField("string"),
        "v": _MockField("string"),
        "extra": _MockField("string"),
    }

    with patch("funcs.utils.table_exists", return_value=True):
        spark.table.return_value.columns = ["id", "v"]

        merge_into_delta(spark, df, "main.default.test_table", match_keys=["id"])

        alter_calls = [str(c.args[0]) for c in spark.sql.call_args_list if "ALTER TABLE" in str(c.args[0])]
        assert any("ADD COLUMNS" in c and "`extra` string" in c for c in alter_calls)


# ===================== merge_into_delta: general MERGE shape =====================

@suppress_print
def test_merge_into_delta_builds_update_when_nonkey_diff_possible():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["id", "val"]
    df.createOrReplaceTempView = MagicMock()

    with patch("funcs.utils.table_exists", return_value=True):
        spark.table.return_value.columns = ["id", "val"]

        merge_into_delta(spark, df, "main.default.test_table", match_keys=["id"])

        sql_calls = [str(c.args[0]) for c in spark.sql.call_args_list]
        merged = [c for c in sql_calls if "MERGE INTO" in c]
        assert merged, "Expected a MERGE INTO statement"
        assert "WHEN NOT MATCHED THEN" in merged[-1]
        assert "WHEN MATCHED AND (" in merged[-1]


# ===================== unify_tables =====================

@suppress_print
def test_unify_tables_unions_and_overwrites_target():
    spark = MagicMock()
    df1 = MagicMock()
    df2 = MagicMock()
    df3 = MagicMock()
    df1.unionByName.return_value = df1
    df1.write.format.return_value.mode.return_value.saveAsTable = MagicMock()

    def _spark_table(name):
        return {"src.one": df1, "src.two": df2, "src.three": df3}[name]
    spark.table.side_effect = _spark_table

    mapping = {"tgt.all": ["src.one", "src.two", "src.three"]}

    unify_tables(spark, mapping)

    assert df1.unionByName.call_count == 2
    df1.write.format.assert_called_with("delta")
    df1.write.format.return_value.mode.assert_called_with("overwrite")
    df1.write.format.return_value.mode.return_value.saveAsTable.assert_called_with("tgt.all")


# ===================== convenience: allow running directly =====================

if __name__ == "__main__":
    test_ensure_dict_key_prefix()
    test_remove_all_angle_brackets()
    test_sql_lit()
    test_quote_3part()
    test_table_exists()
    test_get_table_schema()
    test_generate_table_list()
    test_generate_table_key_data()
    test_generate_fk_script_df()
    test_overwrite_to_delta_writes_with_overwriteSchema()
    test_append_to_delta_writes_with_mergeSchema()
    test_merge_into_delta_raises_on_invalid_target_not_3part()
    test_merge_into_delta_raises_on_empty_match_keys()
    test_merge_into_delta_raises_when_match_key_missing_in_df()
    test_merge_into_delta_creates_table_when_not_exists()
    test_merge_into_delta_scoped_delete_requires_period_col()
    test_merge_into_delta_scoped_delete_executes_delete_with_scope_values_from_df()
    test_merge_into_delta_full_delete_with_filter_executes_delete()
    test_merge_into_delta_pads_missing_source_columns_with_nulls()
    test_merge_into_delta_adds_new_columns_with_inferred_types()
    test_merge_into_delta_builds_update_when_nonkey_diff_possible()
    test_unify_tables_unions_and_overwrites_target()
    
    print("\n✅ All utils tests passed successfully!")

# COMMAND ----------

