import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from libraries.dq_framework.engine.file_selection import parse_patterns, discover_file_names

# ==========================================
# TESTS: parse_patterns
# ==========================================

@pytest.mark.parametrize("input_str, expected", [
    ("abc_*", ["abc_*"]),
    ("abc_*, def", ["abc_*", "def"]),
    ("  spaces , around  ,comma  ", ["spaces", "around", "comma"]),
    ("", []),
    (None, []),
    (",,,", []), # Multiple empty commas
])
def test_parse_patterns(input_str, expected):
    """Verifies string splitting and whitespace cleanup."""
    assert parse_patterns(input_str) == expected


# ==========================================
# TESTS: discover_file_names
# ==========================================

@patch("libraries.dq_framework.engine.file_selection.Path.exists")
def test_discover_file_names_dir_not_found(mock_exists):
    """Raises NotADirectoryError if the target path doesn't exist."""
    mock_exists.return_value = False
    
    with pytest.raises(NotADirectoryError, match="The configuration directory does not exist"):
        discover_file_names("/base", "rel/path", "yaml", include="*")


@patch("libraries.dq_framework.engine.file_selection.Path.exists", return_value=True)
@patch("libraries.dq_framework.engine.file_selection.Path.glob")
def test_discover_file_names_include_logic(mock_glob, mock_exists):
    """Tests basic inclusion and ignoring the 'defaults' file."""
    # Simulate finding 3 files
    file1 = MagicMock(spec=Path); file1.stem = "table_a"; file1.suffix = ".yaml"
    file2 = MagicMock(spec=Path); file2.stem = "table_b"; file2.suffix = ".yaml"
    file3 = MagicMock(spec=Path); file3.stem = "defaults"; file3.suffix = ".yaml"
    mock_glob.return_value = [file1, file2, file3]

    # Include all (*)
    result = discover_file_names("/base", "grp", "yaml", include="*")
    
    assert result == ["table_a", "table_b"] # 'defaults' must be skipped
    assert "defaults" not in result


@patch("libraries.dq_framework.engine.file_selection.Path.exists", return_value=True)
@patch("libraries.dq_framework.engine.file_selection.Path.glob")
def test_discover_file_names_wildcards_and_exclude(mock_glob, mock_exists):
    """Tests complex wildcard matching and exclusion patterns."""
    f1 = MagicMock(spec=Path); f1.stem = "finance_orders"
    f2 = MagicMock(spec=Path); f2.stem = "finance_users"
    f3 = MagicMock(spec=Path); f3.stem = "hr_employees"
    mock_glob.return_value = [f1, f2, f3]

    # Include finance, but exclude users
    result = discover_file_names(
        config_base_dir="/base",
        config_rel_path="grp",
        extension="yaml",
        include="finance_*",
        exclude="*users"
    )
    
    assert result == ["finance_orders"]


@patch("libraries.dq_framework.engine.file_selection.Path.exists", return_value=True)
@patch("libraries.dq_framework.engine.file_selection.Path.glob")
def test_discover_file_names_no_match_raises_value_error(mock_glob, mock_exists):
    """Raises ValueError if include/exclude logic results in 0 files."""
    f1 = MagicMock(spec=Path); f1.stem = "table_a"
    mock_glob.return_value = [f1]

    with pytest.raises(ValueError, match="No configuration files found"):
        discover_file_names("/base", "grp", "yaml", include="table_z")


@patch("libraries.dq_framework.engine.file_selection.Path.exists", return_value=True)
@patch("libraries.dq_framework.engine.file_selection.Path.glob")
def test_discover_file_names_sorting(mock_glob, mock_exists):
    """Ensures results are returned in alphabetical order."""
    f1 = MagicMock(spec=Path); f1.stem = "zebra"
    f2 = MagicMock(spec=Path); f2.stem = "apple"
    f3 = MagicMock(spec=Path); f3.stem = "banana"
    mock_glob.return_value = [f1, f2, f3]

    result = discover_file_names("/base", "grp", "yaml", include="*")
    assert result == ["apple", "banana", "zebra"]