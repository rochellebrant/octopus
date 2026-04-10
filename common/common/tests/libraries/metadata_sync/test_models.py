import sys
import os
import pytest
from unittest.mock import MagicMock

# 1. SETUP REPO PATH
cwd = os.getcwd()
# Go up 4 levels to get from tests/libraries/metadata_sync/ to the repo root
repo_root = os.path.abspath(os.path.join(cwd, "../../../.."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# 2. IMPORTS
from libraries.metadata_sync.models import (
    TableCommentsError,
    TableRunResult,
    RunSummary,
)

# ==========================================
# TESTS: Exceptions
# ==========================================

def test_table_comments_error_is_runtime_error():
    """Ensure the custom exception inherits correctly."""
    assert issubclass(TableCommentsError, RuntimeError)


# ==========================================
# TESTS: TableRunResult
# ==========================================

def test_table_run_result_defaults():
    """Test that lists initialize as empty rather than sharing state."""
    result = TableRunResult(table_key="tbl", full_table_name="cat.db.tbl", status="ok")
    
    assert result.message == ""
    assert result.undefined_columns == []
    assert result.defined_but_missing == []

def test_table_run_result_add_message():
    """Test the message concatenation logic."""
    result = TableRunResult(table_key="tbl", full_table_name="cat.db.tbl", status="ok")
    
    # Adding to empty string
    result.add_message("First message.")
    assert result.message == "First message."
    
    # Appending to existing string
    result.add_message("Second message.")
    assert result.message == "First message. Second message."
    
    # Adding empty/None should safely do nothing
    result.add_message("")
    result.add_message(None)
    assert result.message == "First message. Second message."

def test_table_run_result_mark_warning():
    """Test that warnings only override 'ok' statuses."""
    # Should upgrade 'ok' to 'ok_with_warnings'
    res_ok = TableRunResult("t1", "t1", "ok")
    res_ok.mark_warning()
    assert res_ok.status == "ok_with_warnings"
    
    # Should NOT override an 'error' or 'skipped' status
    res_error = TableRunResult("t2", "t2", "error")
    res_error.mark_warning()
    assert res_error.status == "error"
    
    res_skip = TableRunResult("t3", "t3", "skipped")
    res_skip.mark_warning()
    assert res_skip.status == "skipped"


# ==========================================
# TESTS: RunSummary
# ==========================================

def test_run_summary_add():
    """Test that the add method correctly builds and appends a result object."""
    summary = RunSummary()
    
    # Act
    row = summary.add("tbl", "cat.db.tbl", "ok", "Looking good")
    
    # Assert
    assert isinstance(row, TableRunResult)
    assert len(summary.results) == 1
    assert summary.results[0] == row
    assert row.table_key == "tbl"
    assert row.message == "Looking good"

def test_run_summary_properties_filtering():
    """Test that the @property methods correctly filter the results list."""
    summary = RunSummary()
    
    # Add a mix of statuses
    summary.add("t1", "cat.db.t1", "ok")
    summary.add("t2", "cat.db.t2", "ok_with_warnings")
    summary.add("t3", "cat.db.t3", "error")
    summary.add("t4", "cat.db.t4", "error")
    summary.add("t5", "cat.db.t5", "skipped")
    
    # ok should include both 'ok' and 'ok_with_warnings'
    assert len(summary.ok) == 2
    assert summary.ok[0].table_key == "t1"
    assert summary.ok[1].table_key == "t2"
    
    assert len(summary.errors) == 2
    assert summary.errors[0].table_key == "t3"
    
    assert len(summary.skipped) == 1
    assert summary.skipped[0].table_key == "t5"

def test_run_summary_to_spark_df():
    """Test that results are correctly transformed into a list of tuples for PySpark."""
    summary = RunSummary()
    mock_spark = MagicMock()
    
    # Setup a row with array data to test complex list comprehensions
    row = summary.add("tbl", "cat.db.tbl", "ok_with_warnings", "Test msg")
    row.undefined_columns = ["missing_col_1"]
    row.defined_but_missing = ["missing_col_2"]
    
    # Act
    summary.to_spark_df(mock_spark)
    
    # Assert Spark DataFrame creation was called
    mock_spark.createDataFrame.assert_called_once()
    
    # Extract the args passed to createDataFrame (rows, schema=...)
    args, kwargs = mock_spark.createDataFrame.call_args
    passed_rows = args[0]
    
    # Check that our data was cleanly extracted into the tuple format Spark expects
    assert len(passed_rows) == 1
    assert passed_rows[0] == (
        "tbl", 
        "cat.db.tbl", 
        "ok_with_warnings", 
        "Test msg", 
        ["missing_col_1"], 
        ["missing_col_2"]
    )
    
    # Ensure schema was passed as a kwarg
    assert "schema" in kwargs
    assert kwargs["schema"].names == [
        "table_key", "full_table_name", "status", "message", 
        "undefined_columns", "defined_but_missing"
    ]