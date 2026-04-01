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
from funcs.entry_table_functions import *

def suppress_print(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)
    return wrapper

# COMMAND ----------

# -------------------------------------
# Test A1: load_entry_tables - Table Not in models_config
# -------------------------------------
@suppress_print
def test_load_entry_tables_missing_table():
    try:
        load_entry_tables(
            spark,
            {},  # Empty models_config
            "nonexistent_table",
            "SELECT * FROM {silver_prefix}mytable",
            target_tbl_prefix="tgt_",
            bronze_tbl_prefix="brz_",
            silver_tbl_prefix="slv_",
            gold_tbl_prefix="gld_"
        )
        assert False, "Expected KeyError for table not defined in models_config."
    except KeyError as e:
        assert "Table 'nonexistent_table' not found" in str(e)
    return "test_load_entry_tables_missing_table: PASSED"

# -------------------------------------
# Test A2: load_entry_tables - Missing Columns in Target Table
# -------------------------------------
@suppress_print
def test_load_entry_tables_missing_columns():
    '''
    This unit test is verifying that load_entry_tables() correctly raises a KeyError when required columns are missing in the target table schema.
    A mock configuration is defined; these define the expected set of columns that must be present in the target table.
    A fake schema response function is declared to simulate the behavior of the Spark catalog's spark.catalog.listColumns(table_name) method.
    It simulates that the target table only has one column being "other_col", while the required columns are "col1" and "load_ts" but are missing.
    The real spark.catalog.listColumns method is patched to use the fake response from fake_list_columns.
    This means that any call to spark.catalog.listColumns(...) inside the block now runs fake_list_columns(...).
    The test then calls load_entry_tables() with the mock config and the fake response function.
    If the required columns are missing, a KeyError is raised (as expected). Then, the test checks that the error message includes the expected text.
    If both of these conditions are met, the test passes. If either of these conditions are not met, the test fails.
    '''
    # Step 1: Define a mock config
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }

    # Step 2: Define a fake schema response
    def fake_list_columns(table_name):
        col_mock = MagicMock()
        col_mock.name = "other_col"
        return [col_mock]  # Missing col1 and load_ts

    # Step 3: Patch the real spark.catalog.listColumns method to use the fake response
    with patch.object(target=spark.catalog, attribute="listColumns", side_effect=fake_list_columns):
        # Step 4: Attempt to call load_entry_tables()
        try:
            load_entry_tables(
                spark,
                models_config,
                "test_table",
                "SELECT * FROM {silver_prefix}mytable",
                target_tbl_prefix="tgt_",
                bronze_tbl_prefix="brz_",
                silver_tbl_prefix="slv_",
                gold_tbl_prefix="gld_"
            )
            # Step 5: Verify KeyError is raised. If the function doesn't raise KeyError, the test fails.
            assert False, "Expected KeyError for missing required columns."
        # Step 6: Check that the error message includes the expected text
        except KeyError as e:
            assert "in match_keys does not exist" in str(e)
    # Step 7: Return success message
    return "test_load_entry_tables_missing_columns: PASSED"

# ------------------------------------------------------------
# Test A3: load_entry_tables - Non-Empty Target Table Scenario
# ------------------------------------------------------------
@suppress_print
def test_load_entry_tables_non_empty():
    '''
    This unit test is verifying that load_entry_tables() constructs and runs the correct MERGE SQL when the target tables is not empty (i.e. it already has data and we only want to load new rows).
    A mock configuration is defined; these define the expected set of columns that must be present in the target table, in this case being ["col1", "load_ts"].
    A target table schema is simulated using dummy_columns. Column is a namedtuple that mimics Spark's Column object with a .name attribute.
    A fake fetch column funtion is defined; it returns mocked Column objects using the dummy_columns. It will be used to fake spark.catalog.listColumns(...).
    The spark.catalog.listColumns is patched to replace with fake_list_columns to return correct columns so that column checks pass.
    The check_if_table_is_empty is patched to return (42, "2024-01-01 12:00:00"); simulates non-empty target table (42 rows with load_ts being "2024-01-01 12:00:00").
    These cause load_entry_tables() to build a WHERE clause that filters only newer records (i.e., WHERE load_ts > '2024-01-01 12:00:00').
    A mock SQL query is defined; fake_sql() replaces spark.sql(...) to intercept and record the SQL query by appending it to the list queries_executed.
    It returns a dummy Spark DataFrame with a single row containing ("dummy",) to simulate the result of the SQL query.
    The spark.sql is patched to use fake_sql to intercept and record the SQL query.
    The test then calls load_entry_tables() with the mock config and the fake response function.
    Inputs:
        - table = "test_table"
        - target_tbl_prefix = "tgt_", so final target table is "tgt_test_table"
        - SQL template will render to: SELECT * FROM slv_mytable
    As the table isn't empty (cnt = 42), the function builds:
        - A WHERE clause to only load data after the last timestamp
        - A MERGE INTO SQL statement that will be passed to spark.sql(...)
    The test then checks that the SQL query is recorded in queries_executed and contains the expected SQL statement. If both conditions are met, the test passes. If either of these conditions are not met, the test fails.
    '''
    # Step 1: Provide a mock config
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }

    # Step 2: Define dummy schema columns
    dummy_columns = ["col1", "load_ts", "other_col"]
    Column = namedtuple("Column", ["name"])

    # Step 3: Define fake column fetch function
    def fake_list_columns(_):
        return [Column(col) for col in dummy_columns]

    # Step 4: Patch Spark internals
    with patch.object(spark.catalog, "listColumns", side_effect=fake_list_columns), \
         patch("funcs.entry_table_functions.check_if_table_is_empty", return_value=(42, "2024-01-01 12:00:00")):

        # Step 5: Track SQL queries
        queries_executed = []

        def fake_sql(query):
            queries_executed.append(query)
            return spark.createDataFrame([("dummy",)], StructType([StructField("dummy", StringType(), True)]))

        # Step 6: Patch the SQL executor
        with patch.object(spark, "sql", side_effect=fake_sql):
            load_entry_tables(
                spark,
                models_config,
                "test_table",
                "SELECT * FROM {silver_prefix}mytable",
                target_tbl_prefix="tgt_",
                bronze_tbl_prefix="brz_",
                silver_tbl_prefix="slv_",
                gold_tbl_prefix="gld_"
            )

    # Step 7: Check MERGE SQL was constructed and filtered
    assert any("MERGE INTO tgt_test_table" in q for q in queries_executed)
    assert any("WHERE load_ts > '2024-01-01 12:00:00'" in q for q in queries_executed)
    return "test_load_entry_tables_non_empty: PASSED"

# --------------------------------------------------------
# Test A4: load_entry_tables - Empty Target Table Scenario
# --------------------------------------------------------
@suppress_print
def test_load_entry_tables_empty():
    '''
    This unit test is verifying that load_entry_tables() constructs and runs the correct MERGE SQL when the target tables is empty (i.e. we only want to load all rows).
    A mock configuration is defined; these define the expected set of columns that must be present in the target table, in this case being ["col1", "load_ts"].
    A target table schema is simulated using dummy_columns. Column is a namedtuple that mimics Spark's Column object with a .name attribute.
    A fake fetch column funtion is defined; it returns mocked Column objects using the dummy_columns. It will be used to fake spark.catalog.listColumns(...).
    The spark.catalog.listColumns is patched to replace with fake_list_columns to return correct columns so that column checks pass.
    The check_if_table_is_empty is patched to return (0, None); simulates empty target table.
    These cause load_entry_tables() to build a WHERE clause that returns all records (i.e., WHERE 1=1).
    A mock SQL query is defined; fake_sql() replaces spark.sql(...) to intercept and record the SQL query by appending it to the list queries_executed.
    It returns a dummy Spark DataFrame with a single row containing ("dummy",) to simulate the result of the SQL query.
    The spark.sql is patched to use fake_sql to intercept and record the SQL query.
    The test then calls load_entry_tables() with the mock config and the fake response function.
    Inputs:
        - table = "test_table"
        - target_tbl_prefix = "tgt_", so final target table is "tgt_test_table"
        - SQL template will render to: SELECT * FROM slv_mytable
    As the table is empty (cnt = 0), the function builds:
        - A WHERE clause to load all data
    The test then checks that the SQL query is recorded in queries_executed and contains the expected SQL statement. If both conditions are met, the test passes. If either of these conditions are not met, the test fails.
    '''
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }

    dummy_columns = ["col1", "load_ts"]
    Column = namedtuple("Column", ["name"])

    def fake_list_columns(_):
        return [Column(col) for col in dummy_columns]

    with patch.object(spark.catalog, "listColumns", side_effect=fake_list_columns), \
         patch("funcs.entry_table_functions.check_if_table_is_empty", return_value=(0, None)):

        queries_executed = []

        def fake_sql(query):
            queries_executed.append(query)
            return spark.createDataFrame([("dummy",)], StructType([StructField("dummy", StringType(), True)]))

        with patch.object(spark, "sql", side_effect=fake_sql):
            load_entry_tables(
                spark,
                models_config,
                "test_table",
                "SELECT * FROM {silver_prefix}mytable",
                target_tbl_prefix="tgt_",
                bronze_tbl_prefix="brz_",
                silver_tbl_prefix="slv_",
                gold_tbl_prefix="gld_"
            )

    assert any("WHERE 1 = 1" in q for q in queries_executed)
    return "test_load_entry_tables_empty: PASSED"

# ---------------------------------------------------------------------------
# Test A5: load_entry_tables - Simulate Exception in spark.sql() During MERGE
# ---------------------------------------------------------------------------
@suppress_print
def test_load_entry_tables_merge_exception():
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }
    dummy_columns = ["col1", "load_ts"]
    Column = namedtuple("Column", ["name"])

    def fake_list_columns(_):
        return [Column(col) for col in dummy_columns]

    with patch.object(spark.catalog, "listColumns", side_effect=fake_list_columns), \
         patch("funcs.entry_table_functions.check_if_table_is_empty", return_value=(0, None)):

        with patch.object(spark, "sql", side_effect=Exception("Simulated Spark failure")) as sql_mock:
            try:
                load_entry_tables(
                    spark,
                    models_config,
                    "test_table",
                    "SELECT * FROM {silver_prefix}mytable",
                    target_tbl_prefix="tgt_",
                    bronze_tbl_prefix="brz_",
                    silver_tbl_prefix="slv_",
                    gold_tbl_prefix="gld_"
                )
                assert False, "Exception should have bubbled up as a RuntimeError!"
            except RuntimeError as e:
                assert "Error merging into" in str(e)

            sql_mock.assert_called()
            
    return "test_load_entry_tables_merge_exception: PASSED"

# COMMAND ----------

PATH_TO_MODELS = "./"
SOURCE_TBL_PREFIX = "_DEV"
TARGET_TBL_PREFIX = "_TRG"
BRONZE_TBL_PREFIX = "brz_"
SILVER_TBL_PREFIX = "slv_"
GOLD_TBL_PREFIX = "gld_"

# -------------------------------------------------------
# Test B1: process_table - SQL Branch with "replace" mode
# -------------------------------------------------------
@suppress_print
def test_process_table_sql_replace():
    '''
    This test ensures that:
        - The SQL model path is correctly interpreted.
        - The correct helper functions (init_entry_table_common and load_entry_tables) are called.
        - The "replace" mode leads to a CREATE OR REPLACE TABLE instruction.
        - The function does not fail and does not append the table to failed_tables.
    A mock configuration is defined where the code type is SQL from a file named test_table.py and the table type is "replace" (i.e. should be recreated).
    A mock models dictionary is defined where the key is the base script name and the value is the SQL query.
    A mock failed_tables list is defined.
    The init_entry_table_common and load_entry_tables funtions are patched and replaced with mock objects to check that they were called without executing real logic.
    These mocks are context-managed, so the patching is temporary and reverts after the with block.
    The process_table function is imported and exceuted using all test values. Because of mocking, init_entry_table_common and load_entry_tables don't actually do anything - they just record their call history.
    The mock functions are then checked to see if they were called with the expected arguments:
        - assert_called_once_with() checks if the function was called exactly once, and with the exact argunments (if any are passed in).
        - assert_any_call() checks if the function was called at least once, and with the exact arguments (if any are passed in).
    '''
    # Step 1: Define test inputs
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "replace",
        }
    }
    models_dict = {"test_table": "SELECT * FROM dummy"}
    failed_tables = []

    # Step 2: Patch helper functions
    with patch("funcs.entry_table_functions.init_entry_table_common") as mock_init, \
         patch("funcs.entry_table_functions.load_entry_tables") as mock_load:
        
        # Step 3: Invoke the function
        process_table(
            spark,
            table_config,
            models_dict,
            PATH_TO_MODELS,
            "test_table",
            failed_tables,
            TARGET_TBL_PREFIX,
            BRONZE_TBL_PREFIX,
            SILVER_TBL_PREFIX,
            GOLD_TBL_PREFIX,
        )

    # Step 4: Validate expected behavior
    mock_init.assert_called_once_with(
        spark=spark,
        model="test_table",
        script="SELECT * FROM dummy",
        create_mode="CREATE OR REPLACE TABLE",
        bronze_tbl_prefix=BRONZE_TBL_PREFIX,
        silver_tbl_prefix=SILVER_TBL_PREFIX,
        gold_tbl_prefix=GOLD_TBL_PREFIX,
        target_tbl_prefix=TARGET_TBL_PREFIX,
    )
    mock_load.assert_called_once()
    return "test_process_table_sql_replace: PASSED"

# ------------------------------------------------------
# Test B2: process_table - SQL Branch with "append" mode
# ------------------------------------------------------
@suppress_print
def test_process_table_sql_append():
    '''
    This test ensures that:
        - The SQL model path is correctly interpreted.
        - The correct helper functions (init_entry_table_common and load_entry_tables) are called.
        - The "append" mode leads to a CREATE OR REPLACE TABLE IF EXISTS instruction.
        - The function does not fail and does not append the table to failed_tables.
    A mock configuration is defined where the code type is SQL from a file named test_table.py and the table type is "append".
    A mock models dictionary is defined where the key is the base script name and the value is the SQL query.
    A mock failed_tables list is defined.
    The init_entry_table_common and load_entry_tables funtions are patched and replaced with mock objects to check that they were called without executing real logic.
    These mocks are context-managed, so the patching is temporary and reverts after the with block.
    The process_table function is imported and exceuted using all test values. Because of mocking, init_entry_table_common and load_entry_tables don't actually do anything - they just record their call history.
    The mock functions are then checked to see if they were called with the expected arguments:
        - assert_called_once_with() checks if the function was called exactly once, and with the exact argunments (if any are passed in).
        - assert_any_call() checks if the function was called at least once, and with the exact arguments (if any are passed in).
    '''
    # Step 1: Define test inputs
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "append",
        }
    }
    models_dict = {"test_table": "SELECT * FROM dummy"}
    failed_tables = []

    # Step 2: Patch helper functions
    with patch("funcs.entry_table_functions.init_entry_table_common") as mock_init, \
         patch("funcs.entry_table_functions.load_entry_tables") as mock_load:
             
        # Step 3: Invoke the function
        process_table(
            spark,
            table_config,
            models_dict,
            PATH_TO_MODELS,
            "test_table",
            failed_tables,
            TARGET_TBL_PREFIX,
            BRONZE_TBL_PREFIX,
            SILVER_TBL_PREFIX,
            GOLD_TBL_PREFIX,
        )

    # Step 4: Validate expected behavior
    mock_init.assert_called_once_with(
        spark=spark,
        model="test_table",
        script="SELECT * FROM dummy",
        create_mode="CREATE TABLE IF NOT EXISTS", # specific for append
        bronze_tbl_prefix=BRONZE_TBL_PREFIX,
        silver_tbl_prefix=SILVER_TBL_PREFIX,
        gold_tbl_prefix=GOLD_TBL_PREFIX,
        target_tbl_prefix=TARGET_TBL_PREFIX,
    )
    mock_load.assert_called_once()
    return "test_process_table_sql_nonreplace: PASSED"

# ----------------------------------------
# Test B3: process_table - Notebook Branch
# ----------------------------------------
@suppress_print
def test_process_table_notebook():
    table_config = {
        "test_table": {
            "code_type": "notebook",
            "file_name": "notebook_test.ipynb",
            "table_type": "n/a",
        }
    }
    models_dict = {}
    failed_tables = []

    fake_cwd = "/dummy/repo"
    fake_relpath = "dummy_folder/notebook_test.ipynb"
    expected_path = "dummy_folder/notebook_test"

    with patch("os.getcwd", return_value=fake_cwd), \
         patch("os.path.relpath", return_value=fake_relpath), \
         patch("funcs.entry_table_functions.dbutils") as mock_dbutils:

        mock_dbutils.notebook.run.return_value = "Notebook completed"

        process_table(
            spark,
            table_config,
            models_dict,
            PATH_TO_MODELS,
            "test_table",
            failed_tables,
            TARGET_TBL_PREFIX,
            BRONZE_TBL_PREFIX,
            SILVER_TBL_PREFIX,
            GOLD_TBL_PREFIX,
        )

        # Correct way to access call args
        args, kwargs = mock_dbutils.notebook.run.call_args

        # Path should be first positional arg, extension-less
        assert len(args) >= 1
        assert args[0] == expected_path

        # Keyword args
        assert kwargs["timeout_seconds"] == 1200
        assert kwargs["arguments"]["tgt_table"] == "test_table"
        assert kwargs["arguments"]["bronze_tbl_prefix"] == BRONZE_TBL_PREFIX
        assert kwargs["arguments"]["silver_tbl_prefix"] == SILVER_TBL_PREFIX
        assert kwargs["arguments"]["gold_tbl_prefix"] == GOLD_TBL_PREFIX
        assert kwargs["arguments"]["target_tbl_prefix"] == TARGET_TBL_PREFIX
        assert kwargs["arguments"]["notebook_params"] == "{}"

    assert "test_table" not in failed_tables
    return "test_process_table_notebook: PASSED"

# -------------------------------------
# Test B4: process_table - code_type is unrecognised / unsupported
# -------------------------------------
@suppress_print
def test_process_table_unrecognised_code_type():
    table_config = {
        "test_table": {
            "code_type": "python",
            "file_name": "irrelevant.py",
            "table_type": "n/a",
        }
    }
    models_dict = {}
    failed_tables = []

    with patch("funcs.entry_table_functions.init_entry_table_common") as mock_init, \
         patch("funcs.entry_table_functions.load_entry_tables") as mock_load, \
         patch("funcs.entry_table_functions.dbutils") as mock_dbutils:

        try:
            process_table(
                spark, table_config, models_dict, PATH_TO_MODELS, "test_table", 
                failed_tables, TARGET_TBL_PREFIX, BRONZE_TBL_PREFIX, SILVER_TBL_PREFIX, GOLD_TBL_PREFIX,
            )
        except ValueError:
            pass # We expect this to be raised now!

    assert "test_table" in failed_tables, "Unknown code_type should be logged in failed_tables."
    mock_init.assert_not_called()
    mock_load.assert_not_called()
    mock_dbutils.notebook.run.assert_not_called()
    return "test_process_table_unrecognised_code_type: PASSED"

# -------------------------------------
# Test B5: process_table - Missing script in models_dict
# -------------------------------------
@suppress_print
def test_process_table_missing_model_script():
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "replace",
        }
    }
    models_dict = {}  # Missing script
    failed_tables = []

    try:
        process_table(
            spark, table_config, models_dict, PATH_TO_MODELS, "test_table", 
            failed_tables, TARGET_TBL_PREFIX, BRONZE_TBL_PREFIX, SILVER_TBL_PREFIX, GOLD_TBL_PREFIX,
        )
    except KeyError:
        pass 

    assert "test_table" in failed_tables, "Missing script should cause failure and be logged in failed_tables."
    return "test_process_table_missing_model_script: PASSED"

# -------------------------------------
# Test B6: process_table - Invalid file path in notebook branch
# -------------------------------------
@suppress_print
def test_process_table_notebook_invalid_path_run_failure():
    table_config = {
        "test_table": {
            "code_type": "notebook",
            "file_name": "broken_notebook.ipynb",
            "table_type": "n/a",
        }
    }
    models_dict = {}
    failed_tables = []

    with patch("os.getcwd", return_value="/fake/repo"), \
         patch("os.path.relpath", return_value="notebooks/broken_notebook.ipynb"), \
         patch("funcs.entry_table_functions.dbutils") as mock_dbutils:

        mock_dbutils.notebook.run.side_effect = Exception("Run failed")

        try:
            process_table(
                spark, table_config, models_dict, PATH_TO_MODELS, "test_table", 
                failed_tables, TARGET_TBL_PREFIX, BRONZE_TBL_PREFIX, SILVER_TBL_PREFIX, GOLD_TBL_PREFIX,
            )
        except Exception:
            pass 

    assert "test_table" in failed_tables, "Notebook run failure should log table in failed_tables."
    return "test_process_table_notebook_invalid_path_run_failure: PASSED"

# -------------------------------------
# Test B7: process_table - file_name shorter than 3 chars (edge case in slicing)
# -------------------------------------
@suppress_print
def test_process_table_short_filename():
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "ab",  # Shorter than .py
            "table_type": "replace",
        }
    }
    models_dict = {}
    failed_tables = []

    try:
        process_table(
            spark, table_config, models_dict, PATH_TO_MODELS, "test_table", 
            failed_tables, TARGET_TBL_PREFIX, BRONZE_TBL_PREFIX, SILVER_TBL_PREFIX, GOLD_TBL_PREFIX,
        )
    except ValueError:
        pass

    assert "test_table" in failed_tables, "Improper filename slicing must be logged."
    return "test_process_table_short_filename: PASSED"

# -------------------------------------
# Test B8: process_table - init_entry_table_common raises
# -------------------------------------
@suppress_print
def test_process_table_sql_helper_failure():
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "replace",
        }
    }
    models_dict = {"test_table": "SELECT 1"}
    failed_tables = []

    with patch(
        "funcs.entry_table_functions.init_entry_table_common",
        side_effect=Exception("boom"),
    ) as mock_init, patch(
        "funcs.entry_table_functions.load_entry_tables"
    ) as mock_load:

        try:
            process_table(
                spark, table_config, models_dict, PATH_TO_MODELS, "test_table", 
                failed_tables, TARGET_TBL_PREFIX, BRONZE_TBL_PREFIX, SILVER_TBL_PREFIX, GOLD_TBL_PREFIX,
            )
        except Exception:
            pass 

    assert "test_table" in failed_tables, "Failure inside helper should be surfaced via failed_tables."
    mock_init.assert_called_once()
    mock_load.assert_not_called()
    return "test_process_table_sql_helper_failure: PASSED"

# ----------------------------------------
# Test B9: notebook branch with custom notebook_params
# ----------------------------------------
@suppress_print
def test_process_table_notebook_with_params():
    table_config = {
        "test_table": {
            "code_type": "notebook",
            "file_name": "params_notebook.ipynb",
            "table_type": "n/a",
            "notebook_params": {"threshold": 0.8, "dry_run": True, "listy": [1, 2, 3]},
        }
    }
    models_dict = {}
    failed_tables = []

    with patch("os.getcwd", return_value="/repo"), \
         patch("os.path.relpath", return_value="nb/params_notebook.ipynb"), \
         patch("funcs.entry_table_functions.dbutils") as mock_dbutils:

        mock_dbutils.notebook.run.return_value = "OK"

        process_table(
            spark,
            table_config,
            models_dict,
            PATH_TO_MODELS,
            "test_table",
            failed_tables,
            TARGET_TBL_PREFIX,
            BRONZE_TBL_PREFIX,
            SILVER_TBL_PREFIX,
            GOLD_TBL_PREFIX,
        )

        # Correct unpack
        args, kwargs = mock_dbutils.notebook.run.call_args

        # Path positional arg
        assert args[0] == "nb/params_notebook"

        # KW args
        assert kwargs["timeout_seconds"] == 1200
        import json
        decoded = json.loads(kwargs["arguments"]["notebook_params"])
        assert decoded == {"threshold": 0.8, "dry_run": True, "listy": [1, 2, 3]}
        assert kwargs["arguments"]["tgt_table"] == "test_table"
        assert kwargs["arguments"]["bronze_tbl_prefix"] == BRONZE_TBL_PREFIX
        assert kwargs["arguments"]["silver_tbl_prefix"] == SILVER_TBL_PREFIX
        assert kwargs["arguments"]["gold_tbl_prefix"] == GOLD_TBL_PREFIX
        assert kwargs["arguments"]["target_tbl_prefix"] == TARGET_TBL_PREFIX

    assert "test_table" not in failed_tables
    return "test_process_table_notebook_with_params: PASSED"

# ----------------------------------------
# Test B10: notebook branch with None prefixes coerced to ""
# ----------------------------------------
@suppress_print
def test_process_table_notebook_none_prefixes():
    table_config = {
        "test_table": {
            "code_type": "notebook",
            "file_name": "prefixes.ipynb",
            "table_type": "n/a",
        }
    }
    models_dict = {}
    failed_tables = []

    with patch("os.getcwd", return_value="/repo"), \
         patch("os.path.relpath", return_value="nb/prefixes.ipynb"), \
         patch("funcs.entry_table_functions.dbutils") as mock_dbutils:

        mock_dbutils.notebook.run.return_value = "OK"

        process_table(
            spark,
            table_config,
            models_dict,
            PATH_TO_MODELS,
            "test_table",
            failed_tables,
            TARGET_TBL_PREFIX,
            None,   # bronze_tbl_prefix
            None,   # silver_tbl_prefix
            None,   # gold_tbl_prefix
        )

        args, kwargs = mock_dbutils.notebook.run.call_args
        assert args[0] == "nb/prefixes"

        arguments = kwargs["arguments"]
        assert arguments["bronze_tbl_prefix"] == ""
        assert arguments["silver_tbl_prefix"] == ""
        assert arguments["gold_tbl_prefix"] == ""
        assert arguments["target_tbl_prefix"] == TARGET_TBL_PREFIX  # unchanged
        assert kwargs["timeout_seconds"] == 1200

    assert "test_table" not in failed_tables
    return "test_process_table_notebook_none_prefixes: PASSED"

# -------------------------------------
# Test D1: entry_table_loader - Tests processing multiple levels of tables where table1 & table3 are to succeed while table2 fails
# -------------------------------------
def test_entry_table_loader():
    '''
    This test ensures that:
        - `entry_table_loader` iterates through levels in ascending order,
          calling `process_table` for each table at each level.
        - A fake `process_table` is patched to succeed for table1 & table3 and
          raise for table2, thereby emulating mixed outcomes.
        - `stdout` capture verifies that the per-level “Processing Summary”
          blocks list successes and failures exactly as expected.
        - Level 1 shows “Processed: ['table1'] / Failures: ['table2']”.
        - Level 2 shows “Processed: ['table3']” with no failures.
        - No unhandled exceptions propagate, even when the fake function raises
          for a table.
    '''
    table_configuration = {
        "table1": {"level": 1, "code_type": "sql", "file_name": "table1.py", "table_type": "replace"},
        "table2": {"level": 1, "code_type": "sql", "file_name": "table2.py", "table_type": "replace"},
        "table3": {"level": 2, "code_type": "sql", "file_name": "table3.py", "table_type": "replace"},
    }
    models_dict = {
        "table1": "SELECT * FROM {source}dummy1",
        "table2": "SELECT * FROM {source}dummy2",
        "table3": "SELECT * FROM {source}dummy3",
    }

    output_capture = StringIO()
    original_stdout = sys.stdout
    sys.stdout = output_capture

    def fake_process_table(
        spark,
        table_configuration,
        models_dict,
        path_to_models,
        table,
        failed_tables,
        target_tbl_prefix,
        bronze_tbl_prefix,
        silver_tbl_prefix,
        gold_tbl_prefix,
    ):
        if table == "table2":
            failed_tables.append(table)
            raise Exception("Simulated failure")
        return

    from funcs.entry_table_functions import TableProcessingError

    try:
        with patch("funcs.entry_table_functions.process_table", side_effect=fake_process_table):
            try:
                entry_table_loader(
                    spark,
                    max_level=2,
                    table_configuration=table_configuration,
                    models_dict=models_dict,
                    path_to_models="./",
                    target_tbl_prefix="_TRG",
                    bronze_tbl_prefix="_BRZ",
                    silver_tbl_prefix="_SLV",
                    gold_tbl_prefix="_GLD",
                )
                # If we reach here, no error was raised — that’s now incorrect.
                raise AssertionError("Expected TableProcessingError at end-of-run, but no exception was raised.")
            except TableProcessingError as e:
                # Expected due to table2 failure
                pass
    finally:
        sys.stdout = original_stdout

    output = output_capture.getvalue()

    assert "Level 1 Processing Summary:" in output, "Missing Level 1 summary"
    assert "Processed: ['table1']" in output, "Expected table1 as successful in level 1"
    assert "Failures: ['table2']" in output, "Expected table2 as failed in level 1"

    assert "Level 2 Processing Summary:" in output, "Missing Level 2 summary"
    assert "Processed: ['table3']" in output, "Expected table3 as successful in level 2"

    return "test_entry_table_loader: PASSED"

# -------------------------------------
# Test D1: entry_table_loader - Tests when there are no levels to process (max_level = 0)
# -------------------------------------
@suppress_print
def test_entry_table_loader_zero_max_level():
    '''
    This test ensures that:
        - When `max_level` is 0, `entry_table_loader` performs **no** work and
          prints no “Processing Summary”.
        - Captured stdout is empty (aside from any initial headers the function
          might emit), confirming that the loop conditioned on level never
          executes.
    '''
    table_configuration = {
        "table1": {
            "level": 1,
            "code_type": "sql",
            "file_name": "table1.py",
            "table_type": "replace",
        }
    }
    models_dict = {"table1": "SELECT * FROM dummy1"}

    output_capture = StringIO()
    original_stdout = sys.stdout
    sys.stdout = output_capture

    from funcs.entry_table_functions import entry_table_loader
    entry_table_loader(
        spark,
        max_level=0,
        table_configuration=table_configuration,
        models_dict=models_dict,
        path_to_models=PATH_TO_MODELS,
        target_tbl_prefix=TARGET_TBL_PREFIX,
        bronze_tbl_prefix=BRONZE_TBL_PREFIX,
        silver_tbl_prefix=SILVER_TBL_PREFIX,
        gold_tbl_prefix=GOLD_TBL_PREFIX,
    )

    sys.stdout = original_stdout
    output = output_capture.getvalue()
    print("Captured output:\n", output)

    assert "Processing Summary" not in output
    return "test_entry_table_loader_zero_max_level: PASSED"

# -------------------------------------
# Test D2: entry_table_loader - Tests that when at least 1 table failed, the TableProcessingError is raised
# -------------------------------------
def test_entry_table_loader_end_of_run_failure():
    """
    Mixed outcomes:
      - Level 1: table1 success, table2 failure
      - Level 2: table3 success
    Expect:
      - Per-level summaries printed
      - Final TableProcessingError raised by abort_if_failures at end-of-run
    """
    # Minimal stand-ins; adjust if your code reads more fields
    table_configuration = {
        "table1": {"level": 1, "code_type": "sql", "file_name": "table1.py", "table_type": "replace"},
        "table2": {"level": 1, "code_type": "sql", "file_name": "table2.py", "table_type": "replace"},
        "table3": {"level": 2, "code_type": "sql", "file_name": "table3.py", "table_type": "replace"},
    }
    models_dict = {
        "table1": "SELECT * FROM {source}dummy1",
        "table2": "SELECT * FROM {source}dummy2",
        "table3": "SELECT * FROM {source}dummy3",
    }

    # Capture stdout
    out = StringIO()
    original_stdout = sys.stdout
    sys.stdout = out

    # Fake process_table: fail table2, succeed others
    def fake_process_table(
        spark,
        table_configuration,
        models_dict,
        path_to_models,
        table,
        failed_tables,
        target_tbl_prefix,
        bronze_tbl_prefix,
        silver_tbl_prefix,
        gold_tbl_prefix,
    ):
        if table == "table2":
            failed_tables.append(table)
            # entry_table_loader catches exceptions from futures; we can raise to simulate real failure
            raise Exception("Simulated failure")
        # success path: do nothing

    # Provide a lightweight spark stand-in (your environment may already have `spark`)
    class _SparkStub: pass
    spark = _SparkStub()

    try:
        with patch("funcs.entry_table_functions.process_table", side_effect=fake_process_table):
            entry_table_loader(
                spark,
                max_level=2,
                table_configuration=table_configuration,
                models_dict=models_dict,
                path_to_models="./",
                target_tbl_prefix="_TRG",
                bronze_tbl_prefix="_BRZ",
                silver_tbl_prefix="_SLV",
                gold_tbl_prefix="_GLD",
            )
        # If we get here, no exception was raised → fail
        raise AssertionError("Expected TableProcessingError at end-of-run, but no exception was raised.")
    except TableProcessingError as e:
        pass  # expected
    finally:
        sys.stdout = original_stdout

    output = out.getvalue()

    # Per-level summaries
    assert "Level 1 Processing Summary:" in output, "Missing Level 1 summary"
    assert "Processed: ['table1']" in output, "Expected table1 as successful in level 1"
    assert "Failures: ['table2']" in output, "Expected table2 as failed in level 1"

    assert "Level 2 Processing Summary:" in output, "Missing Level 2 summary"
    assert "Processed: ['table3']" in output, "Expected table3 as successful in level 2"
    assert ("\t No failures." in output) or ("Failures: []" in output), "Expected no failures in level 2"

    return "test_entry_table_loader_end_of_run_failure: PASSED"

# -------------------------------
# Test D3: entry_table_loader -all success (no failure → no exception)
# -------------------------------

def test_entry_table_loader_success_no_failures():
    """
    All tables succeed -> no exception, summaries show no failures.
    """
    table_configuration = {
        "table1": {"level": 1, "code_type": "sql", "file_name": "table1.py", "table_type": "replace"},
        "table2": {"level": 2, "code_type": "sql", "file_name": "table2.py", "table_type": "replace"},
    }
    models_dict = {
        "table1": "SELECT * FROM {source}dummy1",
        "table2": "SELECT * FROM {source}dummy2",
    }

    out = StringIO()
    original_stdout = sys.stdout
    sys.stdout = out

    def fake_process_table(
        spark,
        table_configuration,
        models_dict,
        path_to_models,
        table,
        failed_tables,
        target_tbl_prefix,
        bronze_tbl_prefix,
        silver_tbl_prefix,
        gold_tbl_prefix,
    ):
        # Simulate success: do nothing
        return

    class _SparkStub: pass
    spark = _SparkStub()

    try:
        with patch("funcs.entry_table_functions.process_table", side_effect=fake_process_table):
            entry_table_loader(
                spark,
                max_level=2,
                table_configuration=table_configuration,
                models_dict=models_dict,
                path_to_models="./",
                target_tbl_prefix="_TRG",
                bronze_tbl_prefix="_BRZ",
                silver_tbl_prefix="_SLV",
                gold_tbl_prefix="_GLD",
            )
    except Exception as e:
        raise AssertionError(f"Did not expect exception, but got: {e}")
    finally:
        sys.stdout = original_stdout

    output = out.getvalue()
    assert "Level 1 Processing Summary:" in output
    assert "Processed: ['table1']" in output
    assert ("\t No failures." in output) or ("Failures: []" in output)

    assert "Level 2 Processing Summary:" in output
    assert "Processed: ['table2']" in output
    assert ("\t No failures." in output) or ("Failures: []" in output)

    return "test_entry_table_loader_success_no_failures: PASSED"


# -------------------------------
# Test D3: entry_table_loader - (Optional) zero-level test stays valid (no summaries, no exception)
# -------------------------------

@suppress_print
def test_entry_table_loader_zero_max_level():
    table_configuration = {
        "table1": {"level": 1, "code_type": "sql", "file_name": "table1.py", "table_type": "replace"}
    }
    models_dict = {"table1": "SELECT * FROM dummy1"}

    out = StringIO()
    original_stdout = sys.stdout
    sys.stdout = out

    class _SparkStub: pass
    spark = _SparkStub()

    entry_table_loader(
        spark,
        max_level=0,
        table_configuration=table_configuration,
        models_dict=models_dict,
        path_to_models="./",
        target_tbl_prefix="_TRG",
        bronze_tbl_prefix="_BRZ",
        silver_tbl_prefix="_SLV",
        gold_tbl_prefix="_GLD",
    )

    sys.stdout = original_stdout
    output = out.getvalue()
    assert "Processing Summary" not in output
    return "test_entry_table_loader_zero_max_level: PASSED"

# -------------------------------------
# Test C1: import_sql_scripts
# -------------------------------------
@suppress_print
def test_import_sql_scripts():
    '''
    This test ensures that:
        - `os.listdir` discovers a single Python file in the models directory.
        - `importlib.import_module` is invoked to import that module.
        - A non-dunder attribute from the module populates `MODELS_DICT` under
          the basename key.
        - No extraneous keys are added.
    All filesystem and import operations are fully mocked to avoid disk access.
    '''
    MODELS_DICT = {}
    sample_models_path = "./sample_models"
    fake_files = ["test_models.py"]
    with patch("os.listdir", return_value=fake_files), patch(
        "importlib.import_module"
    ) as fake_import_module:
        fake_module = type("FakeModule", (), {})()
        setattr(fake_module, "test_models", "SELECT HELLO_WORLD FROM XIO_TEAM")
        fake_import_module.return_value = fake_module

        import_sql_scripts(sample_models_path, MODELS_DICT)
        assert "test_models" in MODELS_DICT, "Expected 'test_models' key in MODELS_DICT"
        assert (
            MODELS_DICT["test_models"] == "SELECT HELLO_WORLD FROM XIO_TEAM"
        ), "Content mismatch in MODELS_DICT"
    return "test_import_sql_scripts: PASSED"

# -------------------------------------
# Test C2: Test that __init__.py is skipped
# -------------------------------------
@suppress_print
def test_import_sql_scripts_skips_dunder_init():
    '''
    This test ensures that:
        - Files named `__init__.py` are ignored by `import_sql_scripts`.
        - `importlib.import_module` is **not** called.
        - `models_dict` remains empty.
    '''
    models_dict = {}
    with patch("os.listdir", return_value=["__init__.py"]), \
         patch("importlib.import_module") as mock_import:

        import_sql_scripts("./models", models_dict)
        mock_import.assert_not_called()
        assert models_dict == {}
        return "test_import_sql_scripts_skips_dunder_init: PASSED"
    
# -------------------------------------
# Test C3: Test that some parameter file is skipped on import
# -------------------------------------
@suppress_print
def test_import_sql_scripts_skips_parameter_file():
    '''
    This test ensures that:
        - Files ending in `_parameters.py` (or similar) are excluded from
          import to prevent inadvertently loading config stubs.
        - No mutation occurs on `models_dict`.
    '''
    models_dict = {}
    with patch("os.listdir", return_value=["some_parameters.py"]), \
         patch("importlib.import_module") as mock_import:

        import_sql_scripts("./models", models_dict)
        mock_import.assert_not_called()
        assert models_dict == {}
        return "test_import_sql_scripts_skips_parameter_file: PASSED"

# -------------------------------------
# Test C4: Test that multiple attributes are imported successfully
# -------------------------------------
@suppress_print
def test_import_sql_scripts_imports_multiple_attributes():
    '''
    This test ensures that:
        - When a module exposes *multiple* non-dunder attributes (that are strings), 
          one of them (first found) is inserted into `models_dict` under the module’s
          basename key.
        - Both attribute values are acceptable; the test asserts membership.
    '''
    models_dict = {}
    with patch("os.listdir", return_value=["multi_attr.py"]), \
         patch("importlib.import_module") as mock_import:

        fake_module = MagicMock()
        # CHANGED: These must be strings now, because our code explicitly ignores non-strings!
        setattr(fake_module, "a", "SQL_QUERY_1")
        setattr(fake_module, "b", "SQL_QUERY_2")
        mock_import.return_value = fake_module

        import_sql_scripts("./models", models_dict)

        assert "multi_attr" in models_dict
        assert models_dict["multi_attr"] in ("SQL_QUERY_1", "SQL_QUERY_2") 
        return "test_import_sql_scripts_imports_multiple_attributes: PASSED"
    
# -------------------------------------
# Test C5: Test import failure
# -------------------------------------
@suppress_print
def test_import_sql_scripts_failure():
    '''
    This test ensures that:
        - An `ImportError` raised during `import_module` is caught, logged, and
          results in **no** addition to `models_dict`, allowing the loop to
          continue for other files.
    '''
    models_dict = {}
    with patch("os.listdir", return_value=["bad_module.py"]), \
         patch("importlib.import_module", side_effect=ImportError("Boom")):

        import_sql_scripts("./models", models_dict)
        assert "bad_module" not in models_dict
        return "test_import_sql_scripts_failure: PASSED"

# -------------------------------------
# Test C6: Test importing a module with no non-dunder attributes
# -------------------------------------
@suppress_print
def test_import_sql_scripts_imports_no_attributes():
    '''
    This test ensures that:
        - Modules containing only dunder attributes are skipped.
        - `models_dict` remains unchanged after the import attempt.
    '''
    models_dict = {}
    with patch("os.listdir", return_value=["empty_module.py"]), \
         patch("importlib.import_module") as mock_import:

        fake_module = MagicMock()
        fake_module.__dir__ = MagicMock(return_value=["__name__", "__doc__"])
        mock_import.return_value = fake_module

        import_sql_scripts("./models", models_dict)
        assert models_dict == {}
        

# COMMAND ----------

@suppress_print
def test_setup_prefix_bronze_dlt_production():
    '''
    BRONZE-DLT LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "bronze_dlt",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "",
        "raw_tbl_prefix": "src_db.",
        "bronze_tbl_prefix": "bronze_db.bronze_",
        "silver_entry_tbl_prefix": None,
        "silver_tbl_prefix": None,
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_bronze_dlt_production: PASSED"


@suppress_print
def test_setup_prefix_bronze_dlt_dev():
    '''
    BRONZE-DLT LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "bronze_dlt",
        "dev_",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "dev_",
        "raw_tbl_prefix": "src_db.dev_",
        "bronze_tbl_prefix": "bronze_db.dev_bronze_",
        "silver_entry_tbl_prefix": None,
        "silver_tbl_prefix": None,
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_bronze_dlt_dev: PASSED"


@suppress_print
def test_setup_prefix_silver_entry_production():
    '''
    SILVER-ENTRY LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "silver_entry",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": "bronze_db.bronze_",
        "silver_entry_tbl_prefix": "silver_db.silver_model_",
        "silver_tbl_prefix": None,
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_silver_entry_production: PASSED"


@suppress_print
def test_setup_prefix_silver_entry_dev():
    '''
    SILVER-ENTRY LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "silver_entry",
        "dev_",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "dev_",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": "bronze_db.dev_bronze_",
        "silver_entry_tbl_prefix": "silver_db.dev_silver_model_",
        "silver_tbl_prefix": None,
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_silver_entry_dev: PASSED"


@suppress_print
def test_setup_prefix_silver_dlt_production():
    '''
    SILVER-DLT LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "silver_dlt",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": None,
        "silver_entry_tbl_prefix": "silver_db.silver_model_",
        "silver_tbl_prefix": "silver_db.silver_",
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_silver_dlt_production: PASSED"


@suppress_print
def test_setup_prefix_silver_dlt_dev():
    '''
    SILVER-DLT LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "silver_dlt",
        "dev_",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "dev_",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": None,
        "silver_entry_tbl_prefix": "silver_db.dev_silver_model_",
        "silver_tbl_prefix": "silver_db.dev_silver_",
        "gold_entry_tbl_prefix": None,
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_silver_dlt_dev: PASSED"


@suppress_print
def test_setup_prefix_gold_entry_production():
    '''
    GOLD-ENTRY LAYER
    '''
    from __main__ import setup_prefix
    result = setup_prefix(
        "gold_entry",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": "bronze_db.bronze_",
        "silver_tbl_prefix": "silver_db.silver_",
        "silver_entry_tbl_prefix": None,
        "gold_entry_tbl_prefix": "gold_db.gold_model_",
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_gold_entry_production: PASSED"


@suppress_print
def test_setup_prefix_gold_entry_dev():
    from __main__ import setup_prefix
    result = setup_prefix(
        "gold_entry",
        "dev_",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected = {
        "prefix": "dev_",
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": "bronze_db.dev_bronze_",
        "silver_tbl_prefix": "silver_db.dev_silver_",
        "silver_entry_tbl_prefix": None,
        "gold_entry_tbl_prefix": "gold_db.dev_gold_model_",
    }
    assert result == expected, f"Expected {expected}, got {result}"
    return "test_setup_prefix_gold_entry_dev: PASSED"


@suppress_print
def test_setup_prefix_empty_dev_prefix():
    """
    EDGE-CASE: EMPTY DEV PREFIX
    When development_prefix == '', we treat it like a dev env
    (i.e. NOT production) but with an empty string in the prefix.
    """
    from __main__ import setup_prefix
    result = setup_prefix(
        "silver_dlt",
        "",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected_prefix = ""  # Still empty
    assert result["prefix"] == expected_prefix, "Prefix should be empty string"
    
    assert result["silver_tbl_prefix"] == "silver_db.silver_", (
        "Silver tbl prefix should not include another prefix"
        " when dev prefix is empty"
    )
    return "test_setup_prefix_empty_dev_prefix: PASSED"


@suppress_print
def test_setup_prefix_unknown_layer():
    """
    EDGE-CASE: UNKNOWN LAYER.
    An unrecognised layer should leave all tbl-prefix keys as None
    except for 'prefix', which is still computed.
    """
    from __main__ import setup_prefix
    result = setup_prefix(
        "platinum",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    assert result["prefix"] == "", "Prefix should be empty for production"
    # All other keys should remain None
    other_keys = {
        k: v
        for k, v in result.items()
        if k not in ("prefix",)
    }
    assert all(v is None for v in other_keys.values()), (
        "All non-prefix values should be None for unknown layers"
    )
    return "test_setup_prefix_unknown_layer: PASSED"

# COMMAND ----------

@suppress_print
def test_init_entry_table_common_bronze_success():
    '''
    HAPPY-PATH: bronze-only prefix, CREATE OR REPLACE
    '''
    # ── Arrange
    spark = MagicMock()
    df_mock = MagicMock()
    spark.sql.side_effect = [df_mock, None, None]   # 3 calls in total
    model = "customer"
    script = "SELECT * FROM {bronze_prefix}input_tbl"
    target_prefix = "bronze_db.bronze_"
    create_mode = "CREATE OR REPLACE TABLE"

    # ── Act
    init_entry_table_common(
        spark,
        model=model,
        script=script,
        target_tbl_prefix=target_prefix,
        create_mode=create_mode,
        bronze_tbl_prefix="bronze_db.bronze_"
    )

    # ── Assert
    expected_query = (
        "SELECT * FROM (SELECT * FROM bronze_db.bronze_input_tbl) WHERE 1 = 0"
    )
    assert spark.sql.call_args_list[0][0][0] == expected_query, (
        "First SQL call should build the empty-select wrapper correctly"
    )
    df_mock.createOrReplaceTempView.assert_called_once_with(f"{model}_temp")
    spark.sql.assert_has_calls(
        [
            # 1) temp-view creation
            call(expected_query),
            # 2) table creation
            call(f"{create_mode} {target_prefix}{model} AS SELECT * FROM {model}_temp"),
            # 3) permission grant
            call(f"GRANT MANAGE ON {target_prefix}{model} TO `Sennen XIO Project`"),
        ]
    )
    return "test_init_entry_table_common_bronze_success: PASSED"


@suppress_print
def test_init_entry_table_common_all_prefix_success():
    '''
    HAPPY-PATH: all prefixes supplied, CREATE IF NOT EXISTS
    '''
    spark = MagicMock()
    df_mock = MagicMock()
    spark.sql.side_effect = [df_mock, None, None]

    init_entry_table_common(
        spark,
        model="sales",
        script="""
            SELECT * FROM {bronze_prefix}bronze_tbl
            UNION ALL
            SELECT * FROM {silver_prefix}silver_tbl
            UNION ALL
            SELECT * FROM {gold_prefix}gold_tbl
        """,
        target_tbl_prefix="silver_db.silver_model_",
        create_mode="CREATE TABLE IF NOT EXISTS",
        bronze_tbl_prefix="bronze_db.bronze_",
        silver_tbl_prefix="silver_db.silver_",
        gold_tbl_prefix="gold_db.gold_",
    )

    # Three calls should have been made to spark.sql
    assert spark.sql.call_count == 3, "spark.sql should be invoked three times"
    return "test_init_entry_table_common_all_prefix_success: PASSED"


@suppress_print
def test_init_entry_table_common_error_temp_view():
    '''
    ERROR PATH: failure while creating the temp view
    '''
    spark = MagicMock()
    # First call raises, so nothing else should run
    spark.sql.side_effect = [Exception("syntax error")]

    try:
        init_entry_table_common(
            spark,
            model="faulty",
            script="SELECT 1",
            target_tbl_prefix="bronze_db.bronze_",
            create_mode="CREATE OR REPLACE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Error creating temp view" in str(e), (
            "Exception message should mention temp-view creation"
        )
    return "test_init_entry_table_common_error_temp_view: PASSED"


@suppress_print
def test_init_entry_table_common_error_create_table():
    '''
    ERROR PATH: failure while creating / replacing the table
    '''
    spark = MagicMock()
    df_mock = MagicMock()
    # Fail on second call
    spark.sql.side_effect = [df_mock, Exception("table exists"), None]

    try:
        init_entry_table_common(
            spark,
            model="dup",
            script="SELECT 1",
            target_tbl_prefix="silver_db.silver_",
            create_mode="CREATE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Error creating table" in str(e), (
            "Exception message should mention table creation"
        )
    return "test_init_entry_table_common_error_create_table: PASSED"


@suppress_print
def test_init_entry_table_common_error_grant():
    '''
    ERROR PATH: failure while granting permissions
    '''
    spark = MagicMock()
    df_mock = MagicMock()
    # Fail on third call
    spark.sql.side_effect = [df_mock, None, Exception("no privilege")]

    try:
        init_entry_table_common(
            spark,
            model="secure",
            script="SELECT 1",
            target_tbl_prefix="gold_db.gold_model_",
            create_mode="CREATE OR REPLACE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Failed to grant permission" in str(e), (
            "Exception message should mention permission grant"
        )
    return "test_init_entry_table_common_error_grant: PASSED"


# COMMAND ----------

@suppress_print
def test_check_if_table_is_empty_non_empty():
    '''
    HAPPY-PATH: table has data
    Returns correct (row_count, max_timestamp) when data exists
    '''
    fake_spark = MagicMock()
    fake_row = {"row_count": 7, "max_timestamp": "2025-06-30 12:00:00"}
    fake_spark.sql.return_value.collect.return_value = [fake_row]

    models_cfg = {"orders": {"load_date_column": "load_ts"}}

    result = check_if_table_is_empty(
        fake_spark, "silver_db.silver_", "orders", models_cfg
    )

    assert result == (7, "2025-06-30 12:00:00"), f"Unexpected result: {result}"
    # Verify the constructed SQL contains the full table name and timestamp column
    called_query = fake_spark.sql.call_args[0][0]
    assert "silver_db.silver_orders" in called_query
    assert "MAX(load_ts)" in called_query
    return "test_check_if_table_is_empty_non_empty: PASSED"


@suppress_print
def test_check_if_table_is_empty_empty_override():
    '''
    HAPPY-PATH: table truly empty (row_count=0) timestamp forced to None
    Function should coerce max_timestamp to None when row_count == 0.
    '''
    fake_spark = MagicMock()
    # Simulate a stray timestamp coming back even though the table is empty
    fake_row = {"row_count": 0, "max_timestamp": "2024-01-01 00:00:00"}
    fake_spark.sql.return_value.collect.return_value = [fake_row]

    cfg = {"events": {"load_date_column": "event_ts"}}

    result = check_if_table_is_empty(fake_spark, "bronze_db.bronze_", "events", cfg)

    assert result == (0, None), f"Expected (0, None), got {result}"
    return "test_check_if_table_is_empty_empty_override: PASSED"


@suppress_print
def test_check_if_table_is_empty_table_not_in_config():
    '''
    CONFIG ERROR: table absent from models_config
    '''
    fake_spark = MagicMock()
    cfg = {"users": {"load_date_column": "created_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception for missing table in config"
    except Exception as e:
        assert "not found in models_config" in str(e)
    return "test_check_if_table_is_empty_table_not_in_config: PASSED"


@suppress_print
def test_check_if_table_is_empty_missing_timestamp_col():
    '''
    CONFIG ERROR: load_date_column missing
    '''
    fake_spark = MagicMock()
    cfg = {"orders": {}}  # missing load_date_column

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception for missing load_date_column"
    except Exception as e:
        assert "load_date_column' parameter missing" in str(e)
    return "test_check_if_table_is_empty_missing_timestamp_col: PASSED"


@suppress_print
def test_check_if_table_is_empty_spark_error():
    '''
    RUNTIME ERROR: spark.sql raises (e.g. table doesn't exist)
    '''
    fake_spark = MagicMock()
    fake_spark.sql.side_effect = Exception("Table not found")

    cfg = {"orders": {"load_date_column": "load_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception to bubble up, but none was raised."
    except Exception as e:
        assert "Table not found" in str(e)
        
    return "test_check_if_table_is_empty_spark_error: PASSED"


@suppress_print
def test_check_if_table_is_empty_bad_row_shape():
    '''
    RUNTIME ERROR: malformed row (missing keys) triggers KeyError
    '''
    fake_spark = MagicMock()
    fake_spark.sql.return_value.collect.return_value = [{}]  # no keys!

    cfg = {"orders": {"load_date_column": "load_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected KeyError to bubble up, but none was raised."
    except KeyError:
        pass
        
    return "test_check_if_table_is_empty_bad_row_shape: PASSED"

# COMMAND ----------

@suppress_print
def test_abort_if_failures_noop_on_empty():
    """
    Should NOT raise when the failures list is empty.
    """
    try:
        abort_if_failures([], context="Any")
    except Exception as e:
        raise AssertionError(f"abort_if_failures raised unexpectedly: {e}")
    return "test_abort_if_failures_noop_on_empty: PASSED"

@suppress_print
def test_abort_if_failures_raises_with_count_and_context():
    """
    Should raise TableProcessingError with:
      - correct deduped count
      - context tag in brackets
      - sorted, deduped table list in message
    """
    failed = ["t1", "t2", "t2"]  # duplicate on purpose

    try:
        abort_if_failures(failed, context="Full pipeline run")
        raise AssertionError("Expected TableProcessingError, but no exception was raised.")
    except TableProcessingError as e:
        msg = str(e)
        # Count should reflect de-duped items = 2
        assert "🚨 2 table(s) failed [Full pipeline run]." in msg, f"Unexpected message: {msg}"
        # Names should be sorted, deduped
        assert "Failed tables: t1, t2" in msg, f"Unexpected message: {msg}"
    return "test_abort_if_failures_raises_with_count_and_context: PASSED"


# COMMAND ----------

if __name__ == "__main__":
    # Rochelle's new test
    print(test_setup_prefix_bronze_dlt_production())
    print(test_setup_prefix_bronze_dlt_dev())
    print(test_setup_prefix_silver_entry_production())
    print(test_setup_prefix_silver_entry_dev())
    print(test_setup_prefix_silver_dlt_production())
    print(test_setup_prefix_silver_dlt_dev())
    print(test_setup_prefix_gold_entry_production())
    print(test_setup_prefix_gold_entry_dev())
    print(test_setup_prefix_empty_dev_prefix())
    print(test_setup_prefix_unknown_layer())

    print(test_init_entry_table_common_bronze_success())
    print(test_init_entry_table_common_all_prefix_success())
    print(test_init_entry_table_common_error_temp_view())
    print(test_init_entry_table_common_error_create_table())
    print(test_init_entry_table_common_error_grant())

    print(test_load_entry_tables_missing_table())
    print(test_load_entry_tables_missing_columns())
    print(test_load_entry_tables_non_empty())
    print(test_load_entry_tables_empty())
    print(test_load_entry_tables_merge_exception())

    print(test_process_table_sql_replace())
    print(test_process_table_sql_append())
    print(test_process_table_notebook())
    print(test_process_table_unrecognised_code_type())
    print(test_process_table_missing_model_script())
    print(test_process_table_notebook_invalid_path_run_failure())
    print(test_process_table_short_filename())
    print(test_process_table_sql_helper_failure())
    print(test_process_table_notebook_with_params())
    print(test_process_table_notebook_none_prefixes())

    print(test_import_sql_scripts())
    print(test_import_sql_scripts_skips_dunder_init())
    print(test_import_sql_scripts_skips_parameter_file())
    print(test_import_sql_scripts_imports_multiple_attributes())
    print(test_import_sql_scripts_failure())
    print(test_import_sql_scripts_imports_no_attributes())
    
    print(test_abort_if_failures_noop_on_empty())
    print(test_abort_if_failures_raises_with_count_and_context())

    print(test_entry_table_loader())
    print(test_entry_table_loader_zero_max_level())
    print(test_entry_table_loader_end_of_run_failure())
    print(test_entry_table_loader_success_no_failures())
    print(test_entry_table_loader_zero_max_level())

    print(test_check_if_table_is_empty_non_empty())
    print(test_check_if_table_is_empty_empty_override())
    print(test_check_if_table_is_empty_table_not_in_config())
    print(test_check_if_table_is_empty_missing_timestamp_col())
    print(test_check_if_table_is_empty_spark_error())
    print(test_check_if_table_is_empty_bad_row_shape())

    print(f"\n✅ All tests passed.")

# COMMAND ----------



# COMMAND ----------

