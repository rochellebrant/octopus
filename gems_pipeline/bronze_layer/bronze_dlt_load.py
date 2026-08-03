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
from bronze_table_parameters import table_config as TABLE_CONFIG
from bronze_table_definitions import column_definitions as COLUMN_DEFINITIONS, api_field_mapping as API_FIELD_MAPPING
from bronze_table_rules import table_rules as TABLE_RULES

DEVELOPMENT_PREFIX = spark.conf.get("spark.xio.development_prefix")
DATA_CATALOG = spark.conf.get("xio.data_catalog", "oegen_data_prod_test")
LANDING_SCHEMA = spark.conf.get("xio.landing_schema", "gems_landing")
BRONZE_SCHEMA = spark.conf.get("xio.bronze_schema", "gems_bronze")
SILVER_SCHEMA = spark.conf.get("xio.silver_schema", "gems_silver")
GOLD_SCHEMA = spark.conf.get("xio.gold_schema", "gems_gold")

SOURCE_DATABASE = f"{DATA_CATALOG}.{LANDING_SCHEMA}"
BRONZE_DATABASE = f"{DATA_CATALOG}.{BRONZE_SCHEMA}"
SILVER_DATABASE = f"{DATA_CATALOG}.{SILVER_SCHEMA}"
GOLD_DATABASE = f"{DATA_CATALOG}.{GOLD_SCHEMA}"

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

def unpack_gems_api_landing_data(spark, table, input_prefix, output_prefix, api_mapping, load_ts_column="datalake_ingestion_timestamp"):
    table_name = f"{output_prefix}{table}"
    
    @dlt.table(
        name=table_name,
        comment=f"Unpacked and flattened data with stable ID matching for {table}",
        temporary=False 
    )
    def get_unpacked_data():
        df = spark.readStream.option("ignoreChanges", "true").table(f"{input_prefix}{table}")
        
        exploded_df = df.select(
            "*",
            f.explode(f.expr("cast(raw_data:result as array<variant>)")).alias("record")
        )
        
        cols_to_extract = []
        for clean_col_name, stable_id in api_mapping[table].items():
            if not stable_id.isdigit():
                cols_to_extract.append(
                    f.expr(f"cast(record:{stable_id} as string)").alias(clean_col_name)
                )
            else:
                # 1. Cast Variant to Map
                # 2. Filter the map for keys ending in the ID
                # 3. Grab the first matched value (using 1-based indexing for Spark SQL!)
                extraction_expr = f"""
                    cast(
                        element_at(
                            map_values(
                                map_filter(
                                    cast(record as map<string, variant>), 
                                    (k, v) -> k LIKE '%{stable_id}'
                                )
                            ), 1
                        ) as string
                    )
                """
                cols_to_extract.append(f.expr(extraction_expr).alias(clean_col_name))
        
        metadata_cols = [f.col("table_id"), f.col(load_ts_column)]
        flattened_df = exploded_df.select(*(metadata_cols + cols_to_extract))
        
        # --- BRING IN THE TOMBSTONES ---
        normal_stream = flattened_df.withColumn("is_deleted", f.lit(False))

        try:
            tombstone_stream = spark.readStream.option("skipChangeCommits", "true").table(f"{input_prefix}tombstones_{table}")
            final_stream = normal_stream.unionByName(tombstone_stream, allowMissingColumns=True)
        except Exception as e:
            print(f"No tombstone table found for {table}. Proceeding with active stream only.")
            final_stream = normal_stream
        # ----------------------------------------

        # Deduplicate the incoming stream before hashing or passing to SCD2
        # This eliminates the duplicate API rows while preserving the unique keys
        match_keys = list(api_mapping[table].keys())
        # We group by match keys and the ingestion timestamp to allow legitimate historical updates,
        # but nuke identical twins inside the same payload packet.
        deduped_stream = final_stream.dropDuplicates(match_keys + [load_ts_column])
        
        cols_to_hash = [c for c in final_stream.columns if c not in [load_ts_column, "is_deleted"]]
        concat_expr = f.concat_ws("||", *[f.col(c).cast("string") for c in cols_to_hash])
        final_df = final_stream.withColumn("row_hash", f.sha2(concat_expr, 256))
        
        return final_df

for table in TABLE_CONFIG.keys():

    # Unpack the JSON into a 'raw' unpacked table
    unpack_gems_api_landing_data(
        spark,
        table,
        input_prefix=f"{SOURCE_TBL_PREFIX}landing_",
        output_prefix=f"{TARGET_TBL_PREFIX}unpacked_raw_",
        api_mapping=API_FIELD_MAPPING
    )

    create_table(
        spark,
        table,
        input_prefix=f"{TARGET_TBL_PREFIX}unpacked_raw_",
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
        table_prefix=PREFIX,
        landing_db=SOURCE_DATABASE,
        bronze_db=BRONZE_DATABASE,
        silver_db=SILVER_DATABASE,
        input=f"{TARGET_TBL_PREFIX}sql_join_flag_",
        output=f"{TARGET_TBL_PREFIX}hard_rule_flag_",
    )

    quarantine_layer(
            spark,
            table,
            input=f"{TARGET_TBL_PREFIX}hard_rule_flag_",
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
