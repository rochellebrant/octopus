import pytest
import yaml
from unittest.mock import patch, mock_open
from pathlib import Path

# The run_tests.py handles the sys.path injection, 
# so we can import directly from the library namespace.
from libraries.dq_framework.engine.config_loader import load_yaml, load_combined_config

# ==========================================
# TESTS: load_yaml
# ==========================================

def test_load_yaml_file_not_found():
    """Returns empty dict if path does not exist."""
    with patch("os.path.exists", return_value=False):
        assert load_yaml("fake_path.yaml") == {}

def test_load_yaml_success():
    """Correctly parses valid YAML content."""
    yaml_content = "key: value"
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data=yaml_content)):
        result = load_yaml("path.yaml")
        assert result == {"key": "value"}

def test_load_yaml_syntax_error():
    """Raises ValueError on invalid YAML syntax."""
    invalid_yaml = "key: : value" # Syntax error
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data=invalid_yaml)):
        with pytest.raises(ValueError, match="Syntax error in YAML file"):
            load_yaml("path.yaml")

def test_load_yaml_empty_file():
    """Returns empty dict if file is empty or contains only whitespace."""
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data="")):
        assert load_yaml("path.yaml") == {}


# ==========================================
# TESTS: load_combined_config
# ==========================================

@patch("libraries.dq_framework.engine.config_loader.load_yaml")
def test_load_combined_config_success(mock_load):
    """Test successful merging of defaults and table-specific configs."""
    
    # Define what the two simulated YAML files return
    defaults = {
        "roles": {"admin": "user1"},
        "checks": [{"name": "default_check"}]
    }
    table_cfg = {
        "table_name": "actual_table",
        "roles": {"tester": "user2"},
        "checks": [{"name": "table_check"}]
    }

    # Side effect: first call loads defaults, second loads table_cfg
    mock_load.side_effect = [defaults, table_cfg]

    result = load_combined_config(base_path="/base", config_group="finance", table_yaml_name="orders")

    # Assertions
    assert result["table"]["table_name"] == "actual_table"
    assert result["table"]["config_group"] == "finance"
    
    # Check Role Merging (both should exist)
    assert result["table"]["roles"] == {"admin": "user1", "tester": "user2"}
    
    # Check Checks Merging (list concatenation)
    assert len(result["merged_checks"]) == 2
    assert result["merged_checks"][0]["name"] == "default_check"
    assert result["merged_checks"][1]["name"] == "table_check"

@patch("libraries.dq_framework.engine.config_loader.load_yaml")
def test_load_combined_config_table_not_found(mock_load):
    """Raises FileNotFoundError if table config returns empty (not found)."""
    # defaults returns something, table_cfg returns {}
    mock_load.side_effect = [{"roles": {}}, {}]

    with pytest.raises(FileNotFoundError, match="not found in path"):
        load_combined_config("/base", "grp", "missing_table")

@patch("libraries.dq_framework.engine.config_loader.load_yaml")
def test_load_combined_config_prefers_table_roles(mock_load):
    """Ensures table roles override default roles if keys collide."""
    defaults = {"roles": {"owner": "default_owner"}}
    table_cfg = {"table_name": "t", "roles": {"owner": "table_owner"}}
    
    mock_load.side_effect = [defaults, table_cfg]
    
    result = load_combined_config("/base", "grp", "t")
    assert result["table"]["roles"]["owner"] == "table_owner"

@patch("libraries.dq_framework.engine.config_loader.load_yaml")
def test_load_combined_config_handles_missing_checks(mock_load):
    """Ensures code doesn't crash if 'checks' key is missing from YAML."""
    defaults = {"roles": {}} # No checks
    table_cfg = {"table_name": "t", "roles": {}} # No checks
    
    mock_load.side_effect = [defaults, table_cfg]
    
    result = load_combined_config("/base", "grp", "t")
    assert result["merged_checks"] == []

@patch("libraries.dq_framework.engine.config_loader.load_yaml")
def test_load_combined_config_validation_fail(mock_load):
    """Raises KeyError if table_name is missing and YAML name is None."""
    # We simulate a bad table_cfg and pass None to table_yaml_name (unlikely but tests the logic)
    mock_load.side_effect = [{"roles": {}}, {"roles": {}}] # No table_name in second dict
    
    # Note: the function logic checks table_yaml_name is None, 
    # but in normal use table_yaml_name is usually a string.
    # This matches your specific code line: if "table_name" not in table_cfg and table_yaml_name is None:
    with patch("libraries.dq_framework.engine.config_loader.Path") as mock_path:
        with pytest.raises(KeyError, match="must contain a 'table_name' key"):
            load_combined_config("/base", "grp", None)