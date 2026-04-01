# Databricks notebook source
def get_bundle_env(allowed=("DEV", "")):
    """
    Resolve environment identifier for the pipeline.

    Priority:
    1) Spark config (DLT + cluster jobs)
    2) Job parameters / widgets (serverless notebooks)

    Fails hard if ENV is not explicitly defined or invalid.
    """

    # 1) Try Spark config (DLT, job clusters)
    try:
        env = spark.conf.get("spark.xio.bundle_env")
        if env is not None:
            env = env.strip()
            if env in allowed:
                return env
            raise ValueError(
                f"Invalid ENV value from spark.xio.bundle_env: '{env}'. "
                f"Allowed values: {allowed}"
            )
    except Exception:
        pass

    # 2) Try notebook parameters / widgets (serverless jobs)
    try:
        env = dbutils.widgets.get("BUNDLE_ENV").strip()
        if env in allowed:
            return env
        raise ValueError(
            f"Invalid environment value from widget BUNDLE_ENV: '{env}'. "
            f"Allowed values: {allowed}"
        )
    except Exception:
        pass

    # 3) Fail hard — nothing defined
    raise RuntimeError(
        "ENV is not defined. "
        "You MUST explicitly set ENV via:\n"
        "• Job cluster spark_conf: spark.xio.bundle_env\n"
        "• DLT pipeline configuration: spark.xio.bundle_env\n"
        "• Notebook task base_parameters: BUNDLE_ENV\n"
        "This is intentional to prevent accidental prod writes."
    )
ENV = get_bundle_env()
print(ENV)

# COMMAND ----------

def get_bundle_target(allowed=("staging", "prod", "dev")): 
    # 1) Try Spark config (DLT, job clusters)
    try:
        bundle_target = spark.conf.get("spark.xio.bundle_target")
        if bundle_target is not None:
            bundle_target = bundle_target.strip()
            if bundle_target in allowed:
                return bundle_target
            raise ValueError(
                f"Invalid bundle target value from spark.xio.bundle_target: '{bundle_target}'. "
                f"Allowed values: {allowed}"
            )
    except Exception:
        pass

    # 2) Try notebook parameters / widgets (serverless jobs)
    try:
        bundle_target = dbutils.widgets.get("BUNDLE_TARGET").strip()
        if bundle_target in allowed:
            return bundle_target
        raise ValueError(
            f"Invalid bundle target value from widget BUNDLE_TARGET: '{bundle_target}'. "
            f"Allowed values: {allowed}"
        )
    except Exception:
        pass

    # 3) Fail hard — nothing defined
    raise RuntimeError(
        "Bundle target is not defined. "
        "You MUST explicitly set the bundle target via:\n"
        "• Job cluster spark_conf: spark.xio.bundle_target\n"
        "• DLT pipeline configuration: spark.xio.bundle_target\n"
        "• Notebook task base_parameters: BUNDLE_TARGET\n"
        "This is intentional to prevent accidental prod writes."
    )
bundle_target = get_bundle_target()
print(bundle_target)

# COMMAND ----------

# DBTITLE 1,Standard imports
import sys
import os
import pyspark.sql.functions as f
from datetime import datetime

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
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/cdm_checker')
from pipeline_parameters import DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
from cdm_checker_configuration import config


# --- Import utility functions ---
if IS_FEATURE and not IS_DATABRICKS:
    FUNCTIONS_PATH = f"{SYS_BASE_PATH}/common/funcs"
else:
    if DEVELOPMENT_PREFIX == "production":
        FUNCTIONS_PATH = "/Workspace/XIO/PROD/xio-common-functions/files/funcs"
    else:
        FUNCTIONS_PATH = "/Workspace/XIO/DEV/xio-common-functions/files/funcs"

# FUNCTIONS_PATH = '/Workspace/Shared/oegen-xio-project-eFront/common/funcs/'
print(f">>> Importing funcs from:\n\t{FUNCTIONS_PATH}")
sys.path.append(FUNCTIONS_PATH)
import cdm_checker_functions as cdm_checker
import entry_table_functions as etf

# COMMAND ----------

prefixes = etf.setup_prefix(
    "bronze_dlt", DEVELOPMENT_PREFIX, SOURCE_DATABASE, BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
)
PREFIX = prefixes["prefix"]
RAW_TBL_PREFIX = prefixes["raw_tbl_prefix"]
BRONZE_TBL_PREFIX = prefixes["bronze_tbl_prefix"]

# COMMAND ----------

print(PREFIX)
print(RAW_TBL_PREFIX)
print(BRONZE_TBL_PREFIX)
print(SOURCE_DATABASE)
print(ENV)

# COMMAND ----------

SUMMARY_LIST = []
COLUMN_ORDER = [
    "match_found",
    "cdm_input_method",
    "cdm_source", 
    "data_input_method",
    "data_source", 
    "column_mapping___data_CDM", 
    "join_message",
    "cdm_checker_timestamp"
]
OPTIONAL_COLUMNS = ["secondary_column_mapping___data_CDM", "secondary_join_message"]
SUMMARY_TABLE_SCHEMA = ["cdm_checker_timestamp", "check_name", "join_type", "matches", "missing_in_cdm", "missing_in_data", "missing_in_both", "notes", "saved_to"]

match_col = "match_found"
match_success_str = "Match"
match_fail_str = "No Match"
cdm_prefix = "cdm___"
sql_query_folder = "sql_queries"

# COMMAND ----------

for check_name, check_config in config.items():
    if check_name == "summary_table":
        continue
    
    target_table = cdm_checker.get_full_name_space_target_table(check_name, PREFIX, SOURCE_DATABASE)
    print(target_table)

# COMMAND ----------

summary_table = cdm_checker.get_summary_table(config)
sql_queries_dict = cdm_checker.import_sql_queries(curr_dir)
print()

for check_name, check_config in config.items():
    if check_name == "summary_table":
        continue

    print(f">>> Running check `{check_name}`")

    data_input_method, cdm_input_method = cdm_checker.get_cdm_and_data_input_methods(check_config, check_name)
    
    join_columns_config = cdm_checker.get_join_columns_config(check_config, check_name)
    target_table = cdm_checker.get_full_name_space_target_table(check_name, PREFIX, SOURCE_DATABASE) 
    join_type = cdm_checker.get_join_type(check_config, check_name)
    primary_mapping_list, secondary_mapping_list = cdm_checker.get_column_join_mapping_list(spark, join_columns_config)
    required_cdm_columns, required_data_columns = cdm_checker.get_required_columns(spark, primary_mapping_list, secondary_mapping_list)

    # print(required_cdm_columns)
    # print(required_data_columns)

    data_df = cdm_checker.fetch_table_or_query(spark, data_input_method, check_config, check_name, "Data", sql_queries_dict, raw_tbl_prefix=RAW_TBL_PREFIX, bronze_tbl_prefix=BRONZE_TBL_PREFIX)
    cdm_df = cdm_checker.fetch_table_or_query(spark, cdm_input_method, check_config, check_name, "CDM", sql_queries_dict, raw_tbl_prefix=RAW_TBL_PREFIX, bronze_tbl_prefix=BRONZE_TBL_PREFIX)

    cdm_checker.ensure_required_columns(spark, check_name, set(required_data_columns), data_df, "Data")
    cdm_checker.ensure_required_columns(spark, check_name, set(required_cdm_columns), cdm_df, "CDM")
    
    primary_col_map, secondary_col_map = cdm_checker.get_column_join_mapping_dict(spark, primary_mapping_list, secondary_mapping_list)

    # print(primary_mapping_list)
    # print(primary_col_map)

    data_df = cdm_checker.convert_timestamps(spark, data_df, check_config.get("timestamp_formats", None))
    cdm_df = cdm_checker.add_column_prefix(spark, cdm_df, cdm_prefix)

    result_df = cdm_checker.join_dataframes(spark, data_df, cdm_df, primary_col_map, cdm_prefix, join_type)
    result_df = cdm_checker.add_match_column(spark, result_df, primary_col_map, match_col, match_success_str, match_fail_str, cdm_prefix)

    result_df = cdm_checker.process_primary_conditions(spark, result_df, primary_col_map, cdm_prefix)
    result_df = cdm_checker.add_column_mapping_to_df(spark, result_df, primary_col_map, column_type="primary")
    result_df = cdm_checker.get_result_df_with_columns(spark, result_df, check_config)

    if secondary_mapping_list:
        result_df = cdm_checker.add_column_mapping_to_df(spark, result_df, secondary_col_map, column_type="secondary")
        result_df = cdm_checker.process_secondary_conditions(spark, result_df, secondary_col_map, cdm_prefix, match_success_str)

    result_df = cdm_checker.reorder_columns(spark, result_df, COLUMN_ORDER, OPTIONAL_COLUMNS)

    # spark.sql(f"DROP TABLE IF EXISTS `{target_table}`")
    result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f">>> Saved to `{target_table}`")

    # === Generate Summary Counts ===
    total_count = result_df.count()
    matches_count = result_df.filter(f.col("match_found") == match_success_str).count()
    missing_in_data_count, missing_in_cdm_count, missing_in_both = cdm_checker.count_missing_values(spark, result_df, primary_col_map, cdm_prefix)

    if join_type == "left":
        notes = f"{matches_count} of {total_count} records from Data found in CDM. Left join used — only missing CDM records are counted. `missing_in_data` is not applicable and is set to 0."
    else:
        notes = f"{matches_count} of {total_count} records matched in Data and CDM. {missing_in_data_count} CDM records are missing in Data. {missing_in_cdm_count} Data records are missing in CDM. {missing_in_both} records are missing values in the primary join columns (NULLs) - likely a missing mapping in the corresponding Data-CDM-ID mapping table OR issues in the Data itself."

    SUMMARY_LIST.append({
        "check_name": check_name,
        "join_type": join_type,
        "matches": matches_count,
        "missing_in_cdm": missing_in_cdm_count,
        "missing_in_data": missing_in_data_count,
        "missing_in_both": missing_in_both,
        "notes": notes,
        "saved_to": target_table,
        "cdm_checker_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    print()

summary_df = spark.createDataFrame(SUMMARY_LIST, SUMMARY_TABLE_SCHEMA)
# spark.sql(f"drop table if exists {summary_table}")
summary_df.write.mode("overwrite").saveAsTable(summary_table)
print(f">>> Summary table saved to {summary_table}")

# COMMAND ----------

