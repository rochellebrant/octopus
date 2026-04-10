import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# 1. SETUP REPO PATH
cwd = os.getcwd()
# Go up 4 levels to get from tests/libraries/metadata_sync/ to the repo root
repo_root = os.path.abspath(os.path.join(cwd, "../../../.."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# 2. IMPORTS
from libraries.metadata_sync.models import TableCommentsError
from libraries.metadata_sync.validation import (
    warn,
    info,
    sql_escape,
    validate_column_definitions_shape,
    list_tables_in_schema,
    preflight,
    get_table_columns,
    diff_columns,
)


# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def mock_spark():
    """Returns a fresh mock Spark session for each test."""
    return MagicMock()


# ==========================================
# TESTS: Simple Utilities
# ==========================================

@patch("builtins.print")
def test_warn(mock_print):
    warn("Danger!")
    mock_print.assert_called_once_with(">>> ⚠️ WARNING: Danger!")

@patch("builtins.print")
def test_info(mock_print):
    info("Looking good")
    mock_print.assert_called_once_with(">>> INFO: Looking good")

def test_sql_escape():
    # No quotes
    assert sql_escape("normal string") == "normal string"
    # Single quotes
    assert sql_escape("O'Connor") == "O''Connor"
    # Multiple single quotes
    assert sql_escape("It's a 'test'") == "It''s a ''test''"


# ==========================================
# TESTS: Config Validation
# ==========================================

def test_validate_shape_happy_path():
    """Test that a perfectly shaped dictionary passes without raising."""
    valid_config = {
        "table_name": {
            "columns": {
                "id": {
                    "description": {"comment": "Primary Key"}
                }
            }
        }
    }
    # Should not raise any exceptions
    validate_column_definitions_shape(valid_config)

def test_validate_shape_empty_or_invalid_root():
    with pytest.raises(ValueError, match="must be a non-empty dict"):
        validate_column_definitions_shape({})
        
    with pytest.raises(ValueError, match="must be a non-empty dict"):
        validate_column_definitions_shape(None) # type: ignore

def test_validate_shape_invalid_table_key():
    with pytest.raises(ValueError, match="Invalid table_key"):
        validate_column_definitions_shape({"": {"columns": {}}})
        
    with pytest.raises(ValueError, match="Invalid table_key"):
        validate_column_definitions_shape({123: {"columns": {}}}) # type: ignore

def test_validate_shape_invalid_nested_types():
    # Table def must be a dict
    with pytest.raises(TypeError, match="must be a dict"):
        validate_column_definitions_shape({"tbl": "not a dict"})
        
    # 'columns' must be a dict
    with pytest.raises(TypeError, match="must be a dict"):
        validate_column_definitions_shape({"tbl": {"columns": "not a dict"}})
        
    # Individual column def must be a dict
    with pytest.raises(TypeError, match="must be a dict"):
        validate_column_definitions_shape({"tbl": {"columns": {"id": "not a dict"}}})
        
    # Description must be a dict
    with pytest.raises(TypeError, match="must be a dict"):
        validate_column_definitions_shape({"tbl": {"columns": {"id": {"description": "not a dict"}}}})


# ==========================================
# TESTS: Spark Metadata Extraction
# ==========================================

def test_list_tables_in_schema(mock_spark):
    """Test parsing of the SHOW TABLES command."""
    # Mock spark.sql().select().collect() to return a list of dictionaries
    mock_df = MagicMock()
    mock_df.select.return_value.collect.return_value = [
        {"tableName": "table1"}, 
        {"tableName": "table2"}
    ]
    mock_spark.sql.return_value = mock_df
    
    result = list_tables_in_schema(mock_spark, "cat", "db")
    
    mock_spark.sql.assert_called_once_with("SHOW TABLES IN cat.db")
    assert result == {"table1", "table2"}


def test_get_table_columns_empty(mock_spark):
    """Test behavior when SHOW COLUMNS returns nothing."""
    mock_spark.sql.return_value.collect.return_value = []
    
    assert get_table_columns(mock_spark, "cat.db.tbl") == set()

def test_get_table_columns_with_col_name_key(mock_spark):
    """Test parsing columns when Spark uses 'col_name'."""
    row1, row2 = MagicMock(), MagicMock()
    row1.asDict.return_value = {"col_name": "id"}
    row2.asDict.return_value = {"col_name": "name"}
    
    mock_spark.sql.return_value.collect.return_value = [row1, row2]
    
    result = get_table_columns(mock_spark, "cat.db.tbl")
    assert result == {"id", "name"}

def test_get_table_columns_with_fallback_key(mock_spark):
    """Test parsing columns when Spark uses an unexpected key (fallback)."""
    row1 = MagicMock()
    # No 'col_name' or 'column_name', just 'weird_col_key'
    row1.asDict.return_value = {"weird_col_key": "timestamp"}
    
    mock_spark.sql.return_value.collect.return_value = [row1]
    
    result = get_table_columns(mock_spark, "cat.db.tbl")
    assert result == {"timestamp"}


# ==========================================
# TESTS: Preflight Checks
# ==========================================

def test_preflight_missing_catalog_db(mock_spark):
    with pytest.raises(ValueError, match="catalog and database must be provided"):
        preflight(mock_spark, "", "db", {"tbl": {}})
        
    with pytest.raises(ValueError, match="catalog and database must be provided"):
        preflight(mock_spark, "cat", None, {"tbl": {}}) # type: ignore

@patch("libraries.metadata_sync.validation.list_tables_in_schema")
def test_preflight_spark_error(mock_list_tables, mock_spark):
    mock_list_tables.side_effect = Exception("Cluster offline")
    
    with pytest.raises(TableCommentsError, match="Preflight failed: cannot access schema"):
        preflight(mock_spark, "cat", "db", {"tbl": {"columns": {}}})

@patch("libraries.metadata_sync.validation.list_tables_in_schema")
def test_preflight_require_tables_success(mock_list_tables, mock_spark):
    """Test that preflight passes when require_tables_exist is true AND there is an intersection."""
    # Table exists in both config and Databricks
    mock_list_tables.return_value = {"tbl_a", "tbl_b"}
    config = {"tbl_a": {"columns": {}}}
    
    result = preflight(mock_spark, "cat", "db", config, require_tables_exist=True)
    assert result == {"tbl_a", "tbl_b"}

@patch("libraries.metadata_sync.validation.list_tables_in_schema")
def test_preflight_require_tables_failure(mock_list_tables, mock_spark):
    """Test that preflight fails when require_tables_exist is true BUT there is no intersection."""
    # Databricks has different tables than the config
    mock_list_tables.return_value = {"tbl_c"}
    config = {"tbl_a": {"columns": {}}}
    
    with pytest.raises(TableCommentsError, match="Preflight failed: none of the tables in column_definitions exist"):
        preflight(mock_spark, "cat", "db", config, require_tables_exist=True)


# ==========================================
# TESTS: Set Math (diff_columns)
# ==========================================

def test_diff_columns():
    defined = {"id", "name", "email"}
    actual = {"id", "name", "created_at"}
    
    undefined_in_def, defined_but_missing = diff_columns(defined, actual)
    
    # In table (actual), but missing from definitions
    assert undefined_in_def == {"created_at"}
    
    # In definitions, but missing from table (actual)
    assert defined_but_missing == {"email"}