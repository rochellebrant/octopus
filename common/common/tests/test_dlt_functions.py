# Databricks notebook source
import sys
import os
import importlib
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# 1. THE PROPER MOCK DLT ENVIRONMENT
class MockDLT:
    def __init__(self):
        self.captured = {}
        self.create_streaming_table = MagicMock()
        self.apply_changes = MagicMock()

    def table(self, *args, **kwargs):
        def wrapper(func):
            # Use the explicit DLT name, or the function name if name isn't provided
            name = kwargs.get("name") or func.__name__
            self.captured[name] = func
            return func
        return wrapper

    def append_flow(self, *args, **kwargs):
        return self.table(*args, **kwargs)

# Create the instance and inject it
dlt_instance = MockDLT()
sys.modules["dlt"] = dlt_instance

# 2. SETUP REPO PATHS
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
funcs_path = os.path.join(repo_root, "funcs")

for path in [repo_root, funcs_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

# 3. FORCE RELOAD THE CODE
import funcs.dlt_functions as dlt_funcs
importlib.reload(dlt_funcs)

from funcs.dlt_functions import (
    create_table,
    generate_unioned_materialised_view,
    apply_null_col_checks,
    apply_sql_left_join_checks,
    apply_custom_hard_checks,
    enforce_uniqueness_expectations
)

# 4. TEST DEFINITIONS

def test_create_table_static_schema_routing():
    """Test that an internal table routes to the static schema builder during DAG planning."""
    spark = MagicMock()
    table = "test_table"
    column_definitions = {table: {"columns": {"col1": {}}}}
    dlt_instance.captured.clear()

    with patch("funcs.dlt_functions.dlt_helper.build_static_schema", return_value="mock_static_ddl") as mock_static:
        create_table(spark, table, "in_", "out_", column_definitions=column_definitions)
        
        # Verify static schema builder was called since no schema_source_path was provided
        mock_static.assert_called_once_with(table, column_definitions)
        assert f"out_{table}" in dlt_instance.captured

def test_create_table_live_schema_routing():
    """Test that providing an external schema path routes to the live schema builder."""
    spark = MagicMock()
    table = "test_table"
    column_definitions = {table: {"columns": {"col1": {}}}}
    schema_path = "catalog.schema.raw_test_table"
    dlt_instance.captured.clear()

    with patch("funcs.dlt_functions.dlt_helper.build_live_schema", return_value="mock_live_ddl") as mock_live:
        create_table(
            spark, table, "in_", "out_", 
            column_definitions=column_definitions, 
            schema_source_path=schema_path
        )
        
        # Verify live schema builder was called with the explicit external path
        mock_live.assert_called_once_with(spark, schema_path, table, column_definitions)

def test_create_table_execution_phase():
    """Test the internal execution phase (casting and applying comments) of the created table."""
    spark = MagicMock()
    table = "test_table"
    column_definitions = {table: {}}
    table_config = {table: {}}
    dlt_instance.captured.clear()
    
    # Mock the Spark Stream reader
    mock_df = MagicMock()
    spark.readStream.option.return_value.table.return_value = mock_df

    with patch("funcs.dlt_functions.dlt_helper.cast_columns", return_value="casted_df") as mock_cast, \
         patch("funcs.dlt_functions.dlt_helper.apply_column_comments", return_value="final_df") as mock_comment:
        
        create_table(
            spark, table, "in_", "out_", 
            apply_casting=True, 
            column_definitions=column_definitions, 
            table_config=table_config
        )
        
        # Simulate DLT executing the function
        result_df = dlt_instance.captured[f"out_{table}"]()
        
        # Verify the underlying helper functions were called in the right order
        assert result_df == "final_df"
        mock_cast.assert_called_once_with(mock_df, column_definitions, table_config, table)
        mock_comment.assert_called_once_with("casted_df", table, column_definitions)


def test_generate_unioned_materialised_view():
    spark = MagicMock()
    table = "test_table"
    table_config = {table: {"load_date_column": "ts"}}
    dlt_instance.captured.clear()
    
    spark.read.table.return_value = "dummy_df"
    generate_unioned_materialised_view(spark, table, table_config, "in_", "out_")
    
    # Check the exact name defined in your dlt_functions code
    res_name = f"unifying_materialised_view_{table}"
    assert dlt_instance.captured[res_name]() == "dummy_df"

def test_apply_null_col_checks():
    spark = MagicMock()
    table = "test_table"
    table_config = {table: {"match_keys": ["k"], "non_null_columns": ["c"]}}
    dlt_instance.captured.clear()
    
    spark.readStream.table.return_value = MagicMock()
    
    with patch("funcs.dlt_functions.dlt_helper.add_null_check_flags", return_value="success"):
        apply_null_col_checks(spark, table, table_config, "in_", "out_")
        # Exact DLT name used in dlt_functions: f"{output}{table}"
        assert dlt_instance.captured[f"out_{table}"]() == "success"

def test_apply_sql_left_join_checks():
    spark = MagicMock()
    table = "test_table"
    sql_list = {"c1": "SELECT 1"}
    table_config = {table: {"sql_join_checks": {"c1": {"hard_check": True}}}}
    dlt_instance.captured.clear()
    
    spark.readStream.table.return_value = MagicMock()
    
    with patch("funcs.dlt_functions.dlt_helper.process_sql_left_joins", return_value="success"):
        apply_sql_left_join_checks(spark, table, sql_list, table_config, "in_", "out_", "pref_")
        assert dlt_instance.captured[f"out_{table}"]() == "success"

def test_apply_custom_hard_checks():
    spark = MagicMock()
    table = "test_table"
    rules = {table: {"r1": "col1 IS NULL"}}
    dlt_instance.captured.clear()
    
    spark.readStream.table.return_value = MagicMock()
    
    with patch("funcs.dlt_functions.dlt_helper.apply_hard_check_flags", return_value="success"):
        apply_custom_hard_checks(spark, table, rules, "in_", "out_")
        assert dlt_instance.captured[f"out_{table}"]() == "success"

def test_apply_custom_hard_checks_no_rule():
    spark = MagicMock()
    table = "test_table"
    table_rules = {} 
    dlt_instance.captured.clear()
    
    mock_df = MagicMock()
    spark.readStream.table.return_value = mock_df
    
    apply_custom_hard_checks(spark, table, table_rules, "in_", "out_")
    # Even if no rule, dlt_functions still registers the table
    assert dlt_instance.captured[f"out_{table}"]() == mock_df

def test_enforce_uniqueness_expectations_no_config():
    spark = MagicMock()
    table = "test_table"
    table_config = {table: {}} 
    dlt_instance.captured.clear()
    
    mock_df = MagicMock()
    spark.readStream.table.return_value = mock_df
    
    enforce_uniqueness_expectations(spark, table, table_config, "in_", "out_")
    # This specifically targets the "passthrough" table name in your function
    assert dlt_instance.captured[f"out_{table}"]() == mock_df

# 5. THE RUNNER
test_names = [name for name in globals() if name.startswith("test_") and callable(globals()[name])]

print(f"🚀 Starting DLT Function Test Suite ({len(test_names)} tests)...\n")
print("-" * 50)

passed = 0
failed = 0

for name in test_names:
    try:
        globals()[name]()
        print(f"✅ {name}: PASSED")
        passed += 1
    except Exception as e:
        print(f"❌ {name}: FAILED")
        print(f"   {type(e).__name__}: {str(e)}")
        failed += 1

print("-" * 50)
print(f"Summary: {passed} Passed, {failed} Failed")

# COMMAND ----------

