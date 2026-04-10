# Databricks notebook source
# DBTITLE 1,Standard imports
import sys
import os
import importlib

# COMMAND ----------

# DBTITLE 1,Pipeline folder name
FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "efront_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-efront/dev/files"
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
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/ingestion')
from pipeline_parameters import DEVELOPMENT_PREFIX, SOURCE_DATABASE
from BlackRockEFrontAPI import BlackRockEFrontAPI


# --- Dynamically load table config based on DABs parameter ---
if IS_DATABRICKS:
    # Create the widget with a fallback default, then read it
    dbutils.widgets.text("TABLE_PARAMETERS_MODULE", "api_ingestion_parameters")
    param_module_name = dbutils.widgets.get("TABLE_PARAMETERS_MODULE")
else:
    # Fallback for local script execution
    param_module_name = "api_ingestion_parameters" 

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
    if DEVELOPMENT_PREFIX == "production":
        FUNCTIONS_PATH = "/Workspace/XIO/PROD/xio-common-functions/files/funcs"
    else:
        FUNCTIONS_PATH = "/Workspace/XIO/DEV/xio-common-functions/files/funcs"

print(f">>> Importing funcs from:\n\t{FUNCTIONS_PATH}")

sys.path.append(FUNCTIONS_PATH)
from utils import *

# COMMAND ----------

PREFIX = "" if DEVELOPMENT_PREFIX == "production" else DEVELOPMENT_PREFIX

prefix_message = "(Blank for production mode)" if PREFIX == "" else PREFIX
print(f"{'>>> PRODUCTION MODE ⚠️' if PREFIX == '' else '>>> Dev mode 👷🏼‍♀️🛠️'}")
print(f">>> PREFIX: {prefix_message}")
print(f">>> TARGET DATABASE: {SOURCE_DATABASE}")

# COMMAND ----------

efront_instance = BlackRockEFrontAPI()

for table, config in TABLE_CONFIG.items():
    print(f'>>> Fetching data for "{table}"')
    save_mode = config.get("save_mode") or None
    column_order = config.get("column_order") or None
    match_keys = config.get("match_keys") or None
    period_column  = config.get("period_column") or None

    data = efront_instance.get_data(config["endpoint"])

    if data:
        data = [
            row for row in data 
            if row and any(val is not None and str(val).strip() != "" for val in row.values())
        ]

    if data:
        df_spark = spark.createDataFrame(data)
        df_transformed = efront_instance.transform_data(df_spark)

        if column_order:
            df_transformed = df_transformed.select(*column_order)

        if save_mode == "overwrite":
            overwrite_to_delta(spark, df_transformed, f"{SOURCE_DATABASE}.{PREFIX}{table}")

        if save_mode == "append":
            append_to_delta(spark, df_transformed, f"{SOURCE_DATABASE}.{PREFIX}{table}")

        if save_mode == "merge":
            merge_into_delta(spark,
                             df=df_transformed,
                             target=f"{SOURCE_DATABASE}.{PREFIX}{table}",
                             match_keys=match_keys,
                             delete_mode='scoped',
                             period_column=period_column)
    else:
        print(f'\t⚠️ No data for table "{PREFIX}{table}"')
    print()

# COMMAND ----------

