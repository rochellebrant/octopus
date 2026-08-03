# Databricks notebook source
# DBTITLE 1,Standard imports
import sys
import os
import importlib
import pyspark.sql.functions as f
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Pipeline folder name
FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "efront_pipeline"
REMOTE_PIPELINE_FOLDER_NAME = "xio-efront/files"

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
        PIPELINE_FOLDER_NAME = curr_dir.split(".bundle/", 1)[1].split("/files", 1)[0] + "/files"
    else:
        PIPELINE_FOLDER_NAME = FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME
else:
    PIPELINE_FOLDER_NAME = REMOTE_PIPELINE_FOLDER_NAME


# --- Base path ---
SYS_BASE_PATH = curr_dir.split(f"/{PIPELINE_FOLDER_NAME}")[0]
print(f">>> Base path:\n\t{SYS_BASE_PATH}")


# --- Import pipeline configs ---
print(f">>> Importing pipeline config & models from:\n\t{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}")
PATH_TO_MODELS = f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/silver_layer/silver_models'
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/silver_layer')
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/bronze_layer')
sys.path.append(PATH_TO_MODELS)

# --- Resolve bundle-driven config from job base_parameters ---
if IS_DATABRICKS:
    dbutils.widgets.text("BUNDLE_TARGET", "")
    dbutils.widgets.text("development_prefix", "dev_")
    dbutils.widgets.text("DATA_CATALOG", "oegen_data_prod_test")
    dbutils.widgets.text("LANDING_SCHEMA", "efront_landing")
    dbutils.widgets.text("BRONZE_SCHEMA", "efront_bronze")
    dbutils.widgets.text("SILVER_SCHEMA", "efront_silver")
    dbutils.widgets.text("GOLD_SCHEMA", "efront_gold")
    BUNDLE_TARGET = dbutils.widgets.get("BUNDLE_TARGET")
    DEVELOPMENT_PREFIX = dbutils.widgets.get("development_prefix")
    DATA_CATALOG = dbutils.widgets.get("DATA_CATALOG")
    LANDING_SCHEMA = dbutils.widgets.get("LANDING_SCHEMA")
    BRONZE_SCHEMA = dbutils.widgets.get("BRONZE_SCHEMA")
    SILVER_SCHEMA = dbutils.widgets.get("SILVER_SCHEMA")
    GOLD_SCHEMA = dbutils.widgets.get("GOLD_SCHEMA")
else:
    BUNDLE_TARGET = ""
    DEVELOPMENT_PREFIX = "dev_"
    DATA_CATALOG = "oegen_data_prod_test"
    LANDING_SCHEMA = "efront_landing"
    BRONZE_SCHEMA = "efront_bronze"
    SILVER_SCHEMA = "efront_silver"
    GOLD_SCHEMA = "efront_gold"

SOURCE_DATABASE = f"{DATA_CATALOG}.{LANDING_SCHEMA}"
BRONZE_DATABASE = f"{DATA_CATALOG}.{BRONZE_SCHEMA}"
SILVER_DATABASE = f"{DATA_CATALOG}.{SILVER_SCHEMA}"
GOLD_DATABASE = f"{DATA_CATALOG}.{GOLD_SCHEMA}"


# --- Dynamically load table config based on DABs parameter ---
if IS_DATABRICKS:
    # Create the widget with a fallback default, then read it
    dbutils.widgets.text("TABLE_PARAMETERS_MODULE", "silver_model_parameters")
    param_module_name = dbutils.widgets.get("TABLE_PARAMETERS_MODULE")
else:
    # Fallback for local script execution
    param_module_name = "silver_model_parameters" 

print(f">>> Loading Notebook parameters from module: {param_module_name}")

try:
    param_module = importlib.import_module(param_module_name)
    TABLE_CONFIG = param_module.table_config
except ImportError as e:
    raise ImportError(f"Failed to load parameter module '{param_module_name}'. Ensure it exists in the sys.path.") from e


# --- Import utility functions ---
if IS_FEATURE and not IS_DATABRICKS:
    FUNCTIONS_PATH = f"{SYS_BASE_PATH}/common/funcs"
else:
    if BUNDLE_TARGET == "prod":
        FUNCTIONS_PATH = "/Workspace/XIO/PROD/xio-common-functions/files/funcs"
    else:
        FUNCTIONS_PATH = "/Workspace/XIO/DEV/xio-common-functions/files/funcs"

print(f">>> Importing funcs from:\n\t{FUNCTIONS_PATH}")
sys.path.append(FUNCTIONS_PATH)
import entry_table_functions as etf

# COMMAND ----------

MODELS_DICT = {}
etf.import_sql_scripts(PATH_TO_MODELS, MODELS_DICT)

# COMMAND ----------

# DBTITLE 1,Set up development mode
EXISTING_TABLES_DF = spark.sql(f"show tables in {SILVER_DATABASE}")
EXISTING_TABLES_LIST = list(EXISTING_TABLES_DF.select("tableName").collect())
TABLES = [elem["tableName"] for elem in EXISTING_TABLES_LIST]

MAX_LEVEL = max(TABLE_CONFIG[table]["level"] for table in TABLE_CONFIG)

prefixes = etf.setup_prefix(
    "silver_entry", DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
)
BRZ_TBL_PREFIX = prefixes["bronze_tbl_prefix"]
SLV_TBL_PREFIX = prefixes["silver_entry_tbl_prefix"]
TARGET_TBL_PREFIX = prefixes["silver_entry_tbl_prefix"]

# COMMAND ----------

etf.entry_table_loader(
    spark = spark,
    max_level = MAX_LEVEL,
    table_configuration = TABLE_CONFIG,
    models_dict = MODELS_DICT,
    path_to_models = PATH_TO_MODELS,
    bronze_tbl_prefix = BRZ_TBL_PREFIX,
    silver_tbl_prefix = SLV_TBL_PREFIX,
    target_tbl_prefix = TARGET_TBL_PREFIX
)

# COMMAND ----------

