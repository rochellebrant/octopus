import sys, os

# Set up repo path
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from unittest.mock import patch, MagicMock, call
from collections import namedtuple
from functools import wraps
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from funcs.entry_table_functions import *

spark = SparkSession.builder.getOrCreate()

def suppress_print(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)
    return wrapper

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


# -------------------------------------
# Test A2: load_entry_tables - Missing Columns in Target Table
# -------------------------------------
@suppress_print
def test_load_entry_tables_missing_columns():
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }

    def fake_list_columns(table_name):
        col_mock = MagicMock()
        col_mock.name = "other_col"
        return [col_mock] 

    with patch.object(target=spark.catalog, attribute="listColumns", side_effect=fake_list_columns):
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
            assert False, "Expected KeyError for missing required columns."
        except KeyError as e:
            assert "in match_keys does not exist" in str(e)


# ------------------------------------------------------------
# Test A3: load_entry_tables - Non-Empty Target Table Scenario
# ------------------------------------------------------------
@suppress_print
def test_load_entry_tables_non_empty():
    models_config = {
        "test_table": {"match_keys": ["col1"], "load_date_column": "load_ts"}
    }

    dummy_columns = ["col1", "load_ts", "other_col"]
    Column = namedtuple("Column", ["name"])

    def fake_list_columns(_):
        return [Column(col) for col in dummy_columns]

    with patch.object(spark.catalog, "listColumns", side_effect=fake_list_columns), \
         patch("funcs.entry_table_functions.check_if_table_is_empty", return_value=(42, "2024-01-01 12:00:00")):

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

    assert any("MERGE INTO tgt_test_table" in q for q in queries_executed)
    assert any("WHERE load_ts > '2024-01-01 12:00:00'" in q for q in queries_executed)


# --------------------------------------------------------
# Test A4: load_entry_tables - Empty Target Table Scenario
# --------------------------------------------------------
@suppress_print
def test_load_entry_tables_empty():
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
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "replace",
        }
    }
    models_dict = {"test_table": "SELECT * FROM dummy"}
    failed_tables = []

    with patch("funcs.entry_table_functions.init_entry_table_common") as mock_init, \
         patch("funcs.entry_table_functions.load_entry_tables") as mock_load:
        
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


# ------------------------------------------------------
# Test B2: process_table - SQL Branch with "append" mode
# ------------------------------------------------------
@suppress_print
def test_process_table_sql_append():
    table_config = {
        "test_table": {
            "code_type": "sql",
            "file_name": "test_table.py",
            "table_type": "append",
        }
    }
    models_dict = {"test_table": "SELECT * FROM dummy"}
    failed_tables = []

    with patch("funcs.entry_table_functions.init_entry_table_common") as mock_init, \
         patch("funcs.entry_table_functions.load_entry_tables") as mock_load:
             
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

    mock_init.assert_called_once_with(
        spark=spark,
        model="test_table",
        script="SELECT * FROM dummy",
        create_mode="CREATE TABLE IF NOT EXISTS", 
        bronze_tbl_prefix=BRONZE_TBL_PREFIX,
        silver_tbl_prefix=SILVER_TBL_PREFIX,
        gold_tbl_prefix=GOLD_TBL_PREFIX,
        target_tbl_prefix=TARGET_TBL_PREFIX,
    )
    mock_load.assert_called_once()


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

        args, kwargs = mock_dbutils.notebook.run.call_args

        assert len(args) >= 1
        assert args[0] == expected_path
        assert kwargs["timeout_seconds"] == 1200
        assert kwargs["arguments"]["tgt_table"] == "test_table"
        assert kwargs["arguments"]["bronze_tbl_prefix"] == BRONZE_TBL_PREFIX
        assert kwargs["arguments"]["silver_tbl_prefix"] == SILVER_TBL_PREFIX
        assert kwargs["arguments"]["gold_tbl_prefix"] == GOLD_TBL_PREFIX
        assert kwargs["arguments"]["target_tbl_prefix"] == TARGET_TBL_PREFIX
        assert kwargs["arguments"]["notebook_params"] == "{}"

    assert "test_table" not in failed_tables


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
            pass 

    assert "test_table" in failed_tables, "Unknown code_type should be logged in failed_tables."
    mock_init.assert_not_called()
    mock_load.assert_not_called()
    mock_dbutils.notebook.run.assert_not_called()


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

        args, kwargs = mock_dbutils.notebook.run.call_args

        assert args[0] == "nb/params_notebook"
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
        assert arguments["target_tbl_prefix"] == TARGET_TBL_PREFIX 
        assert kwargs["timeout_seconds"] == 1200

    assert "test_table" not in failed_tables


# -------------------------------------
# Test D1: entry_table_loader 
# -------------------------------------
def test_entry_table_loader():
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
                raise AssertionError("Expected TableProcessingError at end-of-run, but no exception was raised.")
            except TableProcessingError as e:
                pass
    finally:
        sys.stdout = original_stdout

    output = output_capture.getvalue()

    assert "Level 1 Processing Summary:" in output, "Missing Level 1 summary"
    assert "Processed: ['table1']" in output, "Expected table1 as successful in level 1"
    assert "Failures: ['table2']" in output, "Expected table2 as failed in level 1"

    assert "Level 2 Processing Summary:" in output, "Missing Level 2 summary"
    assert "Processed: ['table3']" in output, "Expected table3 as successful in level 2"


# -------------------------------------
# Test D1: entry_table_loader - Tests when there are no levels to process
# -------------------------------------
@suppress_print
def test_entry_table_loader_zero_max_level():
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

    assert "Processing Summary" not in output


# -------------------------------------
# Test D2: entry_table_loader - Tests that when at least 1 table failed
# -------------------------------------
def test_entry_table_loader_end_of_run_failure():
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
        if table == "table2":
            failed_tables.append(table)
            raise Exception("Simulated failure")

    class _SparkStub: pass
    local_spark = _SparkStub()

    try:
        with patch("funcs.entry_table_functions.process_table", side_effect=fake_process_table):
            entry_table_loader(
                local_spark,
                max_level=2,
                table_configuration=table_configuration,
                models_dict=models_dict,
                path_to_models="./",
                target_tbl_prefix="_TRG",
                bronze_tbl_prefix="_BRZ",
                silver_tbl_prefix="_SLV",
                gold_tbl_prefix="_GLD",
            )
        raise AssertionError("Expected TableProcessingError at end-of-run, but no exception was raised.")
    except TableProcessingError as e:
        pass 
    finally:
        sys.stdout = original_stdout

    output = out.getvalue()

    assert "Level 1 Processing Summary:" in output, "Missing Level 1 summary"
    assert "Processed: ['table1']" in output, "Expected table1 as successful in level 1"
    assert "Failures: ['table2']" in output, "Expected table2 as failed in level 1"

    assert "Level 2 Processing Summary:" in output, "Missing Level 2 summary"
    assert "Processed: ['table3']" in output, "Expected table3 as successful in level 2"
    assert ("\t No failures." in output) or ("Failures: []" in output), "Expected no failures in level 2"


# -------------------------------
# Test D3: entry_table_loader -all success (no failure → no exception)
# -------------------------------
def test_entry_table_loader_success_no_failures():
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

    def fake_process_table(*args, **kwargs):
        return

    class _SparkStub: pass
    local_spark = _SparkStub()

    try:
        with patch("funcs.entry_table_functions.process_table", side_effect=fake_process_table):
            entry_table_loader(
                local_spark,
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


# -------------------------------------
# Test C1: import_sql_scripts
# -------------------------------------
@suppress_print
def test_import_sql_scripts():
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


# -------------------------------------
# Test C2: Test that __init__.py is skipped
# -------------------------------------
@suppress_print
def test_import_sql_scripts_skips_dunder_init():
    models_dict = {}
    with patch("os.listdir", return_value=["__init__.py"]), \
         patch("importlib.import_module") as mock_import:

        import_sql_scripts("./models", models_dict)
        mock_import.assert_not_called()
        assert models_dict == {}
    
# -------------------------------------
# Test C3: Test that some parameter file is skipped on import
# -------------------------------------
@suppress_print
def test_import_sql_scripts_skips_parameter_file():
    models_dict = {}
    with patch("os.listdir", return_value=["some_parameters.py"]), \
         patch("importlib.import_module") as mock_import:

        import_sql_scripts("./models", models_dict)
        mock_import.assert_not_called()
        assert models_dict == {}

# -------------------------------------
# Test C4: Test that multiple attributes are imported successfully
# -------------------------------------
@suppress_print
def test_import_sql_scripts_imports_multiple_attributes():
    models_dict = {}
    with patch("os.listdir", return_value=["multi_attr.py"]), \
         patch("importlib.import_module") as mock_import:

        fake_module = MagicMock()
        setattr(fake_module, "a", "SQL_QUERY_1")
        setattr(fake_module, "b", "SQL_QUERY_2")
        mock_import.return_value = fake_module

        import_sql_scripts("./models", models_dict)

        assert "multi_attr" in models_dict
        assert models_dict["multi_attr"] in ("SQL_QUERY_1", "SQL_QUERY_2") 
    
# -------------------------------------
# Test C5: Test import failure
# -------------------------------------
@suppress_print
def test_import_sql_scripts_failure():
    models_dict = {}
    with patch("os.listdir", return_value=["bad_module.py"]), \
         patch("importlib.import_module", side_effect=ImportError("Boom")):

        import_sql_scripts("./models", models_dict)
        assert "bad_module" not in models_dict


# -------------------------------------
# Test C6: Test importing a module with no non-dunder attributes
# -------------------------------------
@suppress_print
def test_import_sql_scripts_imports_no_attributes():
    models_dict = {}
    with patch("os.listdir", return_value=["empty_module.py"]), \
         patch("importlib.import_module") as mock_import:

        fake_module = MagicMock()
        fake_module.__dir__ = MagicMock(return_value=["__name__", "__doc__"])
        mock_import.return_value = fake_module

        import_sql_scripts("./models", models_dict)
        assert models_dict == {}
        
@suppress_print
def test_setup_prefix_bronze_dlt_production():
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


@suppress_print
def test_setup_prefix_bronze_dlt_dev():
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


@suppress_print
def test_setup_prefix_silver_entry_production():
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


@suppress_print
def test_setup_prefix_silver_entry_dev():
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


@suppress_print
def test_setup_prefix_silver_dlt_production():
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


@suppress_print
def test_setup_prefix_silver_dlt_dev():
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


@suppress_print
def test_setup_prefix_gold_entry_production():
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


@suppress_print
def test_setup_prefix_gold_entry_dev():
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


@suppress_print
def test_setup_prefix_empty_dev_prefix():
    result = setup_prefix(
        "silver_dlt",
        "",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    expected_prefix = "" 
    assert result["prefix"] == expected_prefix, "Prefix should be empty string"
    assert result["silver_tbl_prefix"] == "silver_db.silver_"


@suppress_print
def test_setup_prefix_unknown_layer():
    result = setup_prefix(
        "platinum",
        "production",
        "src_db",
        "bronze_db",
        "silver_db",
        "gold_db",
    )
    assert result["prefix"] == "", "Prefix should be empty for production"
    other_keys = {k: v for k, v in result.items() if k not in ("prefix",)}
    assert all(v is None for v in other_keys.values())


@suppress_print
def test_init_entry_table_common_bronze_success():
    spark_mock = MagicMock()
    df_mock = MagicMock()
    spark_mock.sql.side_effect = [df_mock, None, None]  
    model = "customer"
    script = "SELECT * FROM {bronze_prefix}input_tbl"
    target_prefix = "bronze_db.bronze_"
    create_mode = "CREATE OR REPLACE TABLE"

    init_entry_table_common(
        spark_mock,
        model=model,
        script=script,
        target_tbl_prefix=target_prefix,
        create_mode=create_mode,
        bronze_tbl_prefix="bronze_db.bronze_"
    )

    expected_query = (
        "SELECT * FROM (SELECT * FROM bronze_db.bronze_input_tbl) WHERE 1 = 0"
    )
    assert spark_mock.sql.call_args_list[0][0][0] == expected_query
    df_mock.createOrReplaceTempView.assert_called_once_with(f"{model}_temp")
    spark_mock.sql.assert_has_calls(
        [
            call(expected_query),
            call(f"{create_mode} {target_prefix}{model} AS SELECT * FROM {model}_temp"),
            call(f"GRANT MANAGE ON {target_prefix}{model} TO `Sennen XIO Project`"),
        ]
    )


@suppress_print
def test_init_entry_table_common_all_prefix_success():
    spark_mock = MagicMock()
    df_mock = MagicMock()
    spark_mock.sql.side_effect = [df_mock, None, None]

    init_entry_table_common(
        spark_mock,
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

    assert spark_mock.sql.call_count == 3


@suppress_print
def test_init_entry_table_common_error_temp_view():
    spark_mock = MagicMock()
    spark_mock.sql.side_effect = [Exception("syntax error")]

    try:
        init_entry_table_common(
            spark_mock,
            model="faulty",
            script="SELECT 1",
            target_tbl_prefix="bronze_db.bronze_",
            create_mode="CREATE OR REPLACE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Error creating temp view" in str(e)


@suppress_print
def test_init_entry_table_common_error_create_table():
    spark_mock = MagicMock()
    df_mock = MagicMock()
    spark_mock.sql.side_effect = [df_mock, Exception("table exists"), None]

    try:
        init_entry_table_common(
            spark_mock,
            model="dup",
            script="SELECT 1",
            target_tbl_prefix="silver_db.silver_",
            create_mode="CREATE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Error creating table" in str(e)


@suppress_print
def test_init_entry_table_common_error_grant():
    spark_mock = MagicMock()
    df_mock = MagicMock()
    spark_mock.sql.side_effect = [df_mock, None, Exception("no privilege")]

    try:
        init_entry_table_common(
            spark_mock,
            model="secure",
            script="SELECT 1",
            target_tbl_prefix="gold_db.gold_model_",
            create_mode="CREATE OR REPLACE TABLE",
        )
        assert False, "Expected an exception but none was raised"
    except Exception as e:
        assert "Failed to grant permission" in str(e)


@suppress_print
def test_check_if_table_is_empty_non_empty():
    fake_spark = MagicMock()
    fake_row = {"row_count": 7, "max_timestamp": "2025-06-30 12:00:00"}
    fake_spark.sql.return_value.collect.return_value = [fake_row]

    models_cfg = {"orders": {"load_date_column": "load_ts"}}

    result = check_if_table_is_empty(
        fake_spark, "silver_db.silver_", "orders", models_cfg
    )

    assert result == (7, "2025-06-30 12:00:00"), f"Unexpected result: {result}"
    called_query = fake_spark.sql.call_args[0][0]
    assert "silver_db.silver_orders" in called_query
    assert "MAX(load_ts)" in called_query


@suppress_print
def test_check_if_table_is_empty_empty_override():
    fake_spark = MagicMock()
    fake_row = {"row_count": 0, "max_timestamp": "2024-01-01 00:00:00"}
    fake_spark.sql.return_value.collect.return_value = [fake_row]

    cfg = {"events": {"load_date_column": "event_ts"}}

    result = check_if_table_is_empty(fake_spark, "bronze_db.bronze_", "events", cfg)

    assert result == (0, None), f"Expected (0, None), got {result}"


@suppress_print
def test_check_if_table_is_empty_table_not_in_config():
    fake_spark = MagicMock()
    cfg = {"users": {"load_date_column": "created_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception for missing table in config"
    except Exception as e:
        assert "not found in models_config" in str(e)


@suppress_print
def test_check_if_table_is_empty_missing_timestamp_col():
    fake_spark = MagicMock()
    cfg = {"orders": {}}  

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception for missing load_date_column"
    except Exception as e:
        assert "load_date_column' parameter missing" in str(e)


@suppress_print
def test_check_if_table_is_empty_spark_error():
    fake_spark = MagicMock()
    fake_spark.sql.side_effect = Exception("Table not found")

    cfg = {"orders": {"load_date_column": "load_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected exception to bubble up, but none was raised."
    except Exception as e:
        assert "Table not found" in str(e)


@suppress_print
def test_check_if_table_is_empty_bad_row_shape():
    fake_spark = MagicMock()
    fake_spark.sql.return_value.collect.return_value = [{}]  

    cfg = {"orders": {"load_date_column": "load_ts"}}

    try:
        check_if_table_is_empty(fake_spark, "", "orders", cfg)
        assert False, "Expected KeyError to bubble up, but none was raised."
    except KeyError:
        pass


@suppress_print
def test_abort_if_failures_noop_on_empty():
    try:
        abort_if_failures([], context="Any")
    except Exception as e:
        raise AssertionError(f"abort_if_failures raised unexpectedly: {e}")


@suppress_print
def test_abort_if_failures_raises_with_count_and_context():
    failed = ["t1", "t2", "t2"]  

    try:
        abort_if_failures(failed, context="Full pipeline run")
        raise AssertionError("Expected TableProcessingError, but no exception was raised.")
    except TableProcessingError as e:
        msg = str(e)
        assert "🚨 2 table(s) failed [Full pipeline run]." in msg
        assert "Failed tables: t1, t2" in msg