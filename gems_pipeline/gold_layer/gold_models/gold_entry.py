# Databricks notebook source
import sys
import os
import importlib
import pyspark.sql.functions as f
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "gems_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-gems/dev/files"
REMOTE_PIPELINE_FOLDER_NAME = "xio-gems/files"

# COMMAND ----------

# DBTITLE 1,Environment dependable imports
# --- Check environment ---
def is_running_in_databricks():
	return "DATABRICKS_RUNTIME_VERSION" in os.environ

IS_DATABRICKS = is_running_in_databricks()
print(f">>> Running in Databricks:\n\t{IS_DATABRICKS}")


# --- Get current directory ---
if IS_DATABRICKS:
    curr_dir = (
        "/Workspace"
        + dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )

else:
    curr_dir = os.path.abspath(__file__)
print(f">>> Current directory:\n\t{curr_dir}")


# --- Determine feature & bundle flags ---
if curr_dir.startswith("/Workspace/XIO/DEV") or curr_dir.startswith("/Workspace/XIO/PROD"):
    IS_FEATURE = False
    IS_BUNDLE = True
else:
    IS_FEATURE = True
    if curr_dir.startswith("/Workspace/Users") and ".bundle" in curr_dir:
        IS_BUNDLE = True
    else:
        IS_BUNDLE = False

print(f">>> Is bundle:\n\t{IS_BUNDLE}")
print(f">>> Running from feature branch:\n\t{IS_FEATURE}")


# --- Pipeline folder name ---
if IS_FEATURE:
    if IS_BUNDLE:
        PIPELINE_FOLDER_NAME = FEAT_BUNDLE_PIPELINE_FOLDER_NAME
    else:
        PIPELINE_FOLDER_NAME = FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME
else:
    PIPELINE_FOLDER_NAME = REMOTE_PIPELINE_FOLDER_NAME


# --- Base path ---
SYS_BASE_PATH = curr_dir.split(f"/{PIPELINE_FOLDER_NAME}")[0]
print(f">>> Base path:\n\t{SYS_BASE_PATH}")


# --- Import pipeline configs ---
print(f">>> Importing pipeline config & models from:\n\t{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}")
PATH_TO_MODELS = f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/gold_layer/gold_models'
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
sys.path.append(PATH_TO_MODELS)
from pipeline_parameters import *
from gold_model_table_parameters import table_config as TABLE_CONFIG


# --- Import utility functions ---
if IS_FEATURE:
    COMMON_PATH = f"{SYS_BASE_PATH}/common"
else:
    # We point to 'files' because it contains the 'common' package folder
    if DEVELOPMENT_PREFIX == "production":
        COMMON_PATH = "/Workspace/XIO/PROD/xio-common-functions/files"
    else:
        COMMON_PATH = "/Workspace/XIO/DEV/xio-common-functions/files"

if COMMON_PATH not in sys.path:
    sys.path.append(COMMON_PATH)
    print(f">>> Added to sys.path:\n\t{COMMON_PATH}")

import funcs.entry_table_functions as etf
from funcs.utils import ensure_dict_key_prefix
from libraries.metadata_sync.applier import apply_comments_for_all_tables
from gold_model_table_definitions import gold_table_definitions as COLUMN_DEFINITIONS

# COMMAND ----------

MODELS_DICT = {}
etf.import_sql_scripts(PATH_TO_MODELS, MODELS_DICT)

# COMMAND ----------

EXISTING_TABLES_DF = spark.sql(f"show tables in {GOLD_DATABASE}")
EXISTING_TABLES_LIST = list(EXISTING_TABLES_DF.select("tableName").collect())
TABLES = [elem["tableName"] for elem in EXISTING_TABLES_LIST]

MAX_LEVEL = max(TABLE_CONFIG[table]["level"] for table in TABLE_CONFIG)


prefixes = etf.setup_prefix(
    "gold_entry", DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
)
BRZ_TBL_PREFIX = prefixes["bronze_tbl_prefix"]
SLV_TBL_PREFIX = prefixes["silver_tbl_prefix"]
GLD_TBL_PREFIX = prefixes["gold_entry_tbl_prefix"]
TARGET_TBL_PREFIX = prefixes["gold_entry_tbl_prefix"]

TARGET_CATALOG = TARGET_TBL_PREFIX.split(".")[0]
TARGET_DATABASE = TARGET_TBL_PREFIX.split(".")[1]

# COMMAND ----------

etf.entry_table_loader(
    spark = spark,
    max_level = MAX_LEVEL,
    table_configuration = TABLE_CONFIG,
    models_dict = MODELS_DICT,
    path_to_models = PATH_TO_MODELS,
    bronze_tbl_prefix = BRZ_TBL_PREFIX,
    silver_tbl_prefix = SLV_TBL_PREFIX,
    gold_tbl_prefix = GLD_TBL_PREFIX,
    target_tbl_prefix = TARGET_TBL_PREFIX
)

# COMMAND ----------

# Dynamically align COLUMN_DEFINITIONS keys with actual database tables
expected_full_prefix = f"{DEVELOPMENT_PREFIX}gold_model_"
print(f">>> Aligning COLUMN_DEFINITIONS keys with prefix: {expected_full_prefix}")

COLUMN_DEFINITIONS = ensure_dict_key_prefix(COLUMN_DEFINITIONS, expected_full_prefix)

summary = apply_comments_for_all_tables(
    spark=spark,
    catalog=TARGET_CATALOG,
    database=TARGET_DATABASE,
    column_definitions=COLUMN_DEFINITIONS,
    dry_run=False,
    fail_fast=False,            
    fail_on_any_error=True,       
    fail_on_missing_table=False,  
    preflight_require_tables_exist=True, 
)

summary_df = summary.to_spark_df(spark)
display(summary_df)

# COMMAND ----------

