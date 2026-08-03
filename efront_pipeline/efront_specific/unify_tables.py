# Databricks notebook source
# DBTITLE 1,Standard imports
import sys
import os
import importlib

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
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/ingestion')

# --- Resolve bundle-driven config from job base_parameters ---
if IS_DATABRICKS:
    dbutils.widgets.text("BUNDLE_TARGET", "")
    dbutils.widgets.text("development_prefix", "dev_")
    dbutils.widgets.text("DATA_CATALOG", "oegen_data_prod_test")
    dbutils.widgets.text("LANDING_SCHEMA", "efront_landing")
    BUNDLE_TARGET = dbutils.widgets.get("BUNDLE_TARGET")
    DEVELOPMENT_PREFIX = dbutils.widgets.get("development_prefix")
    DATA_CATALOG = dbutils.widgets.get("DATA_CATALOG")
    LANDING_SCHEMA = dbutils.widgets.get("LANDING_SCHEMA")
else:
    BUNDLE_TARGET = ""
    DEVELOPMENT_PREFIX = "dev_"
    DATA_CATALOG = "oegen_data_prod_test"
    LANDING_SCHEMA = "efront_landing"

SOURCE_DATABASE = f"{DATA_CATALOG}.{LANDING_SCHEMA}"

# --- Dynamically load table config based on DABs parameter ---
if IS_DATABRICKS:
    # Create the widget with a fallback default, then read it
    dbutils.widgets.text("TABLE_PARAMETERS_MODULE", "union_parameters")
    param_module_name = dbutils.widgets.get("TABLE_PARAMETERS_MODULE")
else:
    # Fallback for local script execution
    param_module_name = "union_parameters" 

print(f">>> Loading Notebook parameters from module: {param_module_name}")

try:
    param_module = importlib.import_module(param_module_name)
    TABLE_CONFIG = param_module.table_config
    HISTORIC_NOTEBOOK_MAP = getattr(param_module, "historic_notebook_map", {})
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
from utils import apply_env_prefixes, unify_tables, ensure_historic_tables_exist

# COMMAND ----------

PREFIX = "" if DEVELOPMENT_PREFIX == "production" else DEVELOPMENT_PREFIX

prefix_message = "(Blank for production mode)" if PREFIX == "" else PREFIX
print(f"{'>>> PRODUCTION MODE ⚠️' if PREFIX == '' else '>>> Dev mode 👷🏼‍♀️🛠️'}")
print(f">>> PREFIX: {prefix_message}")
print(f">>> TARGET DATABASE: {SOURCE_DATABASE}")

# COMMAND ----------

if IS_DATABRICKS:
    ensure_historic_tables_exist(
        spark,
        dbutils,
        table_config=TABLE_CONFIG,
        historic_notebook_map=HISTORIC_NOTEBOOK_MAP,
        database=SOURCE_DATABASE,
        prefix=PREFIX,
        notebook_params={
            "development_prefix": DEVELOPMENT_PREFIX,
            "DATA_CATALOG": DATA_CATALOG,
            "LANDING_SCHEMA": LANDING_SCHEMA,
        },
    )

PHYSICAL_TABLE_CONFIG = apply_env_prefixes(TABLE_CONFIG, SOURCE_DATABASE, PREFIX)

unify_tables(spark, PHYSICAL_TABLE_CONFIG)

# COMMAND ----------

