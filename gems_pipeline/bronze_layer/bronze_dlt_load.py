# Databricks notebook source
# DBTITLE 1,Standard imports
import dlt
import importlib
import os
import sys
import pyspark.sql.functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

# COMMAND ----------

# DBTITLE 1,Pipeline folder name
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
PATH_TO_SQL_CHECKS = f"{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/bronze_layer/bronze_sql_join_checks"
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
sys.path.append(PATH_TO_SQL_CHECKS)
from pipeline_parameters import DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
from bronze_table_parameters import table_config as TABLE_CONFIG
from bronze_table_definitions import column_definitions as COLUMN_DEFINITIONS, api_field_mapping as API_FIELD_MAPPING
from bronze_table_rules import table_rules as TABLE_RULES


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
from dlt_functions import *
import entry_table_functions as etf

# COMMAND ----------

SQL_CHECKLIST = {}
etf.import_sql_scripts(PATH_TO_SQL_CHECKS, SQL_CHECKLIST)

# COMMAND ----------

# DBTITLE 1,Set up development mode
prefixes = etf.setup_prefix(
    "bronze_dlt", DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
)
PREFIX = prefixes["prefix"]
SOURCE_TBL_PREFIX = prefixes["raw_tbl_prefix"]

TARGET_TBL_PREFIX = prefixes["bronze_tbl_prefix"]
TABLES_NON_SCD2 = []
for table, config in TABLE_CONFIG.items():
    if config.get("scd2_enabled") is False:
        TABLES_NON_SCD2.append(table)

# COMMAND ----------

# DBTITLE 1,Run pipeline
dlt.create_streaming_table(f"{TARGET_TBL_PREFIX}raw_quarantine_log")

for table in TABLE_CONFIG.keys():

    # Unpack the JSON into a 'raw' unpacked table
    unpack_api_landing_data(
        spark,
        table,
        input_prefix=f"{DEVELOPMENT_PREFIX}landing_",
        output_prefix=f"{TARGET_TBL_PREFIX}unpacked_raw_",
        api_mapping=API_FIELD_MAPPING
    )

    match_keys = TABLE_CONFIG[table]["match_keys"] 
    
    deduplicate_raw_stream(
        spark,
        table,
        input_prefix=f"{TARGET_TBL_PREFIX}unpacked_raw_",
        output_prefix=f"{TARGET_TBL_PREFIX}unpacked_", 
        match_keys=match_keys # Pass the list here
    )

    create_table(
        spark,
        table,
        input_prefix=f"{TARGET_TBL_PREFIX}unpacked_",
        output_prefix=f"{TARGET_TBL_PREFIX}casted_",
        temporary=True,
        comment=f"Temporary landing table for ingesting and schema-aligning data from source {SOURCE_TBL_PREFIX}{table}.",
        apply_casting=True,
        column_definitions=COLUMN_DEFINITIONS,
        table_config=TABLE_CONFIG,
    )

    apply_null_col_checks(
        spark,
        table,
        TABLE_CONFIG,
        input=f"{TARGET_TBL_PREFIX}casted_",
        output=f"{TARGET_TBL_PREFIX}null_flag_",
    )

    apply_sql_left_join_checks(
        spark,
        table,
        SQL_CHECKLIST,
        TABLE_CONFIG,
        input=f"{TARGET_TBL_PREFIX}null_flag_",
        output=f"{TARGET_TBL_PREFIX}sql_join_flag_",
        table_prefix=PREFIX
    )

    apply_custom_hard_checks(
        spark,
        table,
        TABLE_RULES,
        input=f"{TARGET_TBL_PREFIX}sql_join_flag_",
        output=f"{TARGET_TBL_PREFIX}hard_rule_flag_",
    )

    enforce_uniqueness_expectations(
        spark,
        table,
        TABLE_CONFIG,
        input=f"{TARGET_TBL_PREFIX}hard_rule_flag_",
        output=f"{TARGET_TBL_PREFIX}unique_pass_",
    )

    quarantine_layer(
            spark,
            table,
            input=f"{TARGET_TBL_PREFIX}unique_pass_",
            output_quarantine=f"{TARGET_TBL_PREFIX}quarantine_",
            output_clean=f"{TARGET_TBL_PREFIX}clean_",
        )

    if table in TABLES_NON_SCD2:
        create_table(
            spark,
            table,
            input_prefix = f"{TARGET_TBL_PREFIX}clean_",
            output_prefix = f"{TARGET_TBL_PREFIX}",
            temporary = False,
            column_definitions = COLUMN_DEFINITIONS
            )
        
    else:
        generate_scd_tables(
            spark,
            source_table_prefix=f"{TARGET_TBL_PREFIX}clean_",
            table=table,
            table_config=TABLE_CONFIG,
            column_defintions=COLUMN_DEFINITIONS,
            input=f"{TARGET_TBL_PREFIX}clean_",
            output=f"{TARGET_TBL_PREFIX}",
            defined_schema=True
        )

    generate_raw_quarantine_log(
        spark,
        table=table,
        table_config=TABLE_CONFIG,
        input=f"{TARGET_TBL_PREFIX}quarantine_",
        output=f"{TARGET_TBL_PREFIX}raw_quarantine_log",
    )
