# Databricks notebook source
# DBTITLE 1,Standard imports
import importlib
import os
import sys
import pyspark.sql.functions as f
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
from silver_table_parameters import table_config as TABLE_CONFIG

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
        
    print(f"--- Processing Silver {table} ---")
    match_keys = config["match_keys"]
    load_ts_col = config["load_date_column"]
    
    silver_target_table_name = f"{SCHEMA}.{DEVELOPMENT_PREFIX}silver_{table}"
    silver_source_table_name = f"{SCHEMA}.{DEVELOPMENT_PREFIX}silver_model_{table}"
    tombstone_table = f"{SCHEMA}.{DEVELOPMENT_PREFIX}silver_model_tombstones_{table}"

    # Ensure the source exists first as we need its schema!
    if not spark.catalog.tableExists(silver_source_table_name):
        print(f"⚠️ Source snapshot {silver_source_table_name} does not exist yet. Skipping.")
        continue

    # Target check / Day 1 Initialization
    if not spark.catalog.tableExists(silver_target_table_name):
        print(f"⚠️ Target table {silver_target_table_name} does not exist yet.")
        
        # Pre-initialise empty tombstone table for DLT Day 1
        if not spark.catalog.tableExists(tombstone_table):
            print(f"🛠️ Pre-initializing empty tombstone table for Silver DLT Day 1: {tombstone_table}")
            
            # Clone exact schema directly from source table
            empty_df = spark.read.table(silver_source_table_name).select(*match_keys).limit(0)
            
            final_empty_df = (
                empty_df.withColumn("is_deleted", f.lit(True))
                .withColumn("datalake_ingestion_timestamp", f.current_timestamp())
                .withColumn("table_id", f.lit(-1))
                .withColumn(load_ts_col, f.current_timestamp())
            )
            
            (final_empty_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(tombstone_table))
        
        print("Skipping anti-join until Silver target is populated.")
        continue
    
    # Grab Active Silver DLT Data
    active_silver_df = spark.read.table(silver_target_table_name).filter(f.col("__END_AT").isNull())

    # Grab the Newest Snapshot from Silver Entry
    snapshot_df = spark.read.table(silver_source_table_name)
    
    generate_and_append_tombstones(
        spark=spark,
        active_records_df=active_silver_df, 
        latest_snapshot_df=snapshot_df,
        match_keys=match_keys,
        tombstone_target_table=tombstone_table,
        sequence_col=load_ts_col
    )

# COMMAND ----------

