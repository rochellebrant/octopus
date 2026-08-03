# Databricks notebook source
# DBTITLE 1,Standard imports
import importlib
import os
import sys
import pyspark.sql.functions as f
import datetime
from pyspark.sql.types import StructType, StructField, StringType

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
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
from bronze_table_parameters import table_config as TABLE_CONFIG
from bronze_table_definitions import api_field_mapping

DEVELOPMENT_PREFIX = dbutils.widgets.get("development_prefix").strip().lower()
BUNDLE_TARGET = dbutils.widgets.get("bundle_target").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
DATABASE = dbutils.widgets.get("database").strip()
SCHEMA = f"{CATALOG}.{DATABASE}"

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
from dlt_helper import generate_and_append_tombstones

# COMMAND ----------

# DBTITLE 1,Set up development mode
for table, config in TABLE_CONFIG.items():
    if not config.get("scd2_enabled") or not config.get("apply_as_deletes_expr"):
        continue
        
    print(f"--- Processing {table} ---")
    match_keys = config["match_keys"]
    load_date_column = config["load_date_column"]
    
    bronze_table_name = f"{SCHEMA}.{DEVELOPMENT_PREFIX}bronze_{table}"
    landing_table_name = f"{SCHEMA}.{DEVELOPMENT_PREFIX}landing_{table}"
    
    # This prevents Spark's lazy evaluation from throwing late errors on Day 1
    if not spark.catalog.tableExists(bronze_table_name):
        print(f"⚠️ Target table {bronze_table_name} does not exist yet.")
        tombstone_table = f"{SCHEMA}.{DEVELOPMENT_PREFIX}landing_tombstones_{table}"
        if not spark.catalog.tableExists(tombstone_table):
            print(f"🛠️ Pre-initializing empty tombstone table for DLT Day 1: {tombstone_table}")
            
            # We create an empty dataframe with the exact match_key schema
            empty_schema = StructType([StructField(k, StringType(), True) for k in match_keys])
            empty_df = spark.createDataFrame([], empty_schema)
            
            final_empty_df = (
                empty_df.withColumn("is_deleted", f.lit(True))
                .withColumn("datalake_ingestion_timestamp", f.current_timestamp())
                .withColumn("table_id", f.lit(-1))
                .withColumn(load_date_column, f.current_timestamp())
            )
            
            (final_empty_df.write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .saveAsTable(tombstone_table))
        
        print("Skipping anti-join until Bronze is populated.")
        continue
        
    if not spark.catalog.tableExists(landing_table_name):
        print(f"⚠️ Source snapshot {landing_table_name} does not exist yet. Skipping.")
        continue
    
    # Grab Active Bronze Data
    active_bronze_df = spark.read.table(bronze_table_name).filter(f.col("__END_AT").isNull())

    # Grab today's date string dynamically (matches the partition format)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # Read directly from today's snapshot partition
    latest_snapshot_df = (
        spark.read.table(landing_table_name)
        .filter(f.col("snapshot_date") == f.lit(today_str))
        .select(f.explode(f.expr("cast(raw_data:result as array<variant>)")).alias("record"))
    )

    latest_snapshot_df = latest_snapshot_df.withColumn(
        "record_map", 
        f.from_json(f.col("record").cast("string"), "map<string,string>")
    )
    
    for clean_col_name in match_keys:
        api_field = api_field_mapping[table].get(clean_col_name)
        if not api_field:
            continue
            
        # ILIKE for case-insensitive key matching to prevent NULL extractions
        extract_expr = f"""
            try_element_at(
                map_values(map_filter(record_map, (k, v) -> k ILIKE '%{api_field}')), 
                1
            )
        """
        latest_snapshot_df = latest_snapshot_df.withColumn(clean_col_name, f.expr(extract_expr))
        
        try:
            # Force the snapshot extraction to a clean string, stripping any ".0" decimals
            latest_snapshot_df = latest_snapshot_df.withColumn(
                clean_col_name, 
                f.regexp_replace(f.col(clean_col_name).cast("string"), "\\.0$", "")
            )
            # Ensure the Bronze side is also a clean string for the Anti-Join
            active_bronze_df = active_bronze_df.withColumn(
                clean_col_name,
                f.regexp_replace(f.col(clean_col_name).cast("string"), "\\.0$", "")
            )
        except Exception as e:
            print(f"⚠️ Error aligning column {clean_col_name}: {e}")

    tombstone_table = f"{SCHEMA}.{DEVELOPMENT_PREFIX}landing_tombstones_{table}"
    
    generate_and_append_tombstones(
        spark=spark,
        active_records_df=active_bronze_df,
        latest_snapshot_df=latest_snapshot_df,
        match_keys=match_keys,
        tombstone_target_table=tombstone_table,
        sequence_col=load_date_column,
    )

# COMMAND ----------

