import sys
import os
import pytest
from unittest.mock import MagicMock, call, patch

# 1. SETUP REPO PATH
cwd = os.getcwd()
# Go up 4 levels to get from tests/libraries/metadata_sync/ to the repo root
repo_root = os.path.abspath(os.path.join(cwd, "../../../.."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# 2. IMPORTS
from libraries.metadata_sync.applier import (
    _exec_sql,
    apply_comments_from_definitions,
    apply_comments_for_all_tables,
)
from libraries.metadata_sync.models import TableCommentsError

# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def mock_spark():
    """Returns a fresh mock Spark session for each test."""
    return MagicMock()

@pytest.fixture
def mock_result_row():
    """Returns a mock TableRunResult row for testing state mutations."""
    row = MagicMock()
    row.undefined_columns = []
    row.defined_but_missing = []
    return row

# ==========================================
# TESTS: _exec_sql
# ==========================================

@patch("libraries.metadata_sync.applier.info")
def test_exec_sql_dry_run(mock_info, mock_spark):
    """Test that dry_run logs the SQL but doesn't execute it."""
    _exec_sql(mock_spark, "SELECT 1", dry_run=True)
    
    mock_info.assert_called_once_with("SELECT 1")
    mock_spark.sql.assert_not_called()

@patch("libraries.metadata_sync.applier.info")
def test_exec_sql_execute(mock_info, mock_spark):
    """Test that non-dry_run actually executes the SQL via spark."""
    _exec_sql(mock_spark, "SELECT 1", dry_run=False)
    
    mock_spark.sql.assert_called_once_with("SELECT 1")
    mock_info.assert_not_called()

# ==========================================
# TESTS: apply_comments_from_definitions
# ==========================================

@patch("libraries.metadata_sync.applier.warn")
def test_apply_comments_missing_table_in_definitions(mock_warn, mock_spark, mock_result_row):
    """Test behavior when the requested table isn't in the definitions dict."""
    apply_comments_from_definitions(
        mock_spark, "cat.db.tbl", "tbl", column_definitions={}, result_row=mock_result_row
    )
    
    mock_warn.assert_called_with("Table not found in definitions: tbl")
    mock_result_row.mark_warning.assert_called_once()
    mock_result_row.add_message.assert_called_once_with("Table missing from definitions.")
    mock_spark.sql.assert_not_called()

@patch("libraries.metadata_sync.applier.sql_escape", side_effect=lambda x: x)
@patch("libraries.metadata_sync.applier.diff_columns", return_value=(set(), set()))
@patch("libraries.metadata_sync.applier.get_table_columns", return_value={"col1"})
def test_apply_comments_happy_path(
    mock_get_cols, mock_diff, mock_escape, mock_spark, mock_result_row
):
    """Test a fully successful application of both table and column comments."""
    col_defs = {
        "tbl": {
            "table": {"comment": "Main table comment"},
            "columns": {
                "col1": {
                    "description": {
                        "comment": "Col comment",
                        "unit": "kg",
                        "long_name": "Weight"
                    }
                }
            }
        }
    }
    
    apply_comments_from_definitions(
        mock_spark, "cat.db.tbl", "tbl", col_defs, mock_result_row, dry_run=False
    )
    
    # Assert table comment was executed
    assert mock_spark.sql.call_count == 2
    table_sql = mock_spark.sql.call_args_list[0][0][0]
    col_sql = mock_spark.sql.call_args_list[1][0][0]
    
    assert "COMMENT ON TABLE cat.db.tbl IS 'Main table comment'" in table_sql
    assert "COMMENT ON COLUMN cat.db.tbl.col1 IS 'Weight. Col comment" in col_sql
    assert "UNIT: kg'" in col_sql

@patch("libraries.metadata_sync.applier.warn")
@patch("libraries.metadata_sync.applier.diff_columns")
@patch("libraries.metadata_sync.applier.get_table_columns", return_value={"col1", "col2"})
def test_apply_comments_column_mismatches(
    mock_get_cols, mock_diff, mock_warn, mock_spark, mock_result_row
):
    """Test that warnings are added when definitions and actual table columns differ."""
    # diff_columns returns (undefined_in_def, defined_but_missing)
    mock_diff.return_value = ({"col2"}, {"col3"}) 
    
    col_defs = {"tbl": {"columns": {"col1": {}, "col3": {}}}}
    
    apply_comments_from_definitions(
        mock_spark, "cat.db.tbl", "tbl", col_defs, mock_result_row
    )
    
    # Check that warnings were applied to the result row
    assert mock_result_row.mark_warning.call_count >= 2
    assert mock_result_row.undefined_columns == ["col2"]
    assert mock_result_row.defined_but_missing == ["col3"]

# ==========================================
# TESTS: apply_comments_for_all_tables
# ==========================================

@patch("libraries.metadata_sync.applier.apply_comments_from_definitions")
@patch("libraries.metadata_sync.applier.preflight")
def test_apply_all_happy_path(mock_preflight, mock_apply_def, mock_spark):
    """Test successful orchestration of multiple tables."""
    # Preflight says both tables exist in Databricks
    mock_preflight.return_value = {"tbl1", "tbl2"} 
    col_defs = {"tbl1": {}, "tbl2": {}}
    
    summary = apply_comments_for_all_tables(mock_spark, "cat", "db", col_defs)
    
    assert len(summary.ok) == 2
    assert len(summary.errors) == 0
    assert len(summary.skipped) == 0
    assert mock_apply_def.call_count == 2

@patch("libraries.metadata_sync.applier.preflight", return_value=set()) # Preflight finds NO tables
def test_apply_all_missing_table_skip(mock_preflight, mock_spark):
    """Test behavior when a table is missing, but fail_on_missing_table=False."""
    col_defs = {"tbl1": {}}
    
    summary = apply_comments_for_all_tables(
        mock_spark, "cat", "db", col_defs, fail_on_missing_table=False
    )
    
    assert len(summary.skipped) == 1
    assert len(summary.ok) == 0

@patch("libraries.metadata_sync.applier.preflight", return_value=set())
def test_apply_all_missing_table_fail(mock_preflight, mock_spark):
    """Test behavior when a table is missing, and fail_on_missing_table=True."""
    col_defs = {"tbl1": {}}
    
    with pytest.raises(TableCommentsError, match="Comment application failed with 1 error"):
        apply_comments_for_all_tables(
            mock_spark, "cat", "db", col_defs, 
            fail_on_missing_table=True, fail_on_any_error=True
        )

@patch("libraries.metadata_sync.applier.apply_comments_from_definitions", side_effect=Exception("API Outage"))
@patch("libraries.metadata_sync.applier.preflight", return_value={"tbl1", "tbl2"})
def test_apply_all_exception_handling_fail_fast(mock_preflight, mock_apply_def, mock_spark):
    """Test that fail_fast instantly halts the loop on the first exception."""
    col_defs = {"tbl1": {}, "tbl2": {}}
    
    with pytest.raises(TableCommentsError):
        apply_comments_for_all_tables(
            mock_spark, "cat", "db", col_defs, 
            fail_fast=True, fail_on_any_error=True
        )
        
    # Because fail_fast is True, the loop breaks after tbl1 crashes. 
    # It should never attempt tbl2.
    mock_apply_def.assert_called_once()