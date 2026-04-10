# Databricks notebook source
import json 
from pyspark.sql import functions as F, window as W
import os
import sys

# COMMAND ----------

# dbutils.widgets.text("tgt_table", "")
# dbutils.widgets.text("target_tbl_prefix", "")
# dbutils.widgets.text("bronze_tbl_prefix", "")
# dbutils.widgets.text("silver_tbl_prefix", "")
# dbutils.widgets.text("gold_tbl_prefix", "")
# dbutils.widgets.text("notebook_params", "")

tgt_table         = dbutils.widgets.get("tgt_table")
target_tbl_prefix = dbutils.widgets.get("target_tbl_prefix")
bronze_tbl_prefix = dbutils.widgets.get("bronze_tbl_prefix")
silver_tbl_prefix = dbutils.widgets.get("silver_tbl_prefix")
gold_tbl_prefix   = dbutils.widgets.get("gold_tbl_prefix")

notebook_params_raw = dbutils.widgets.get("notebook_params")
notebook_params     = json.loads(notebook_params_raw) if notebook_params_raw else {}

match_keys       = notebook_params.get("match_keys", [])
load_date_column = notebook_params.get("load_date_column", "refresh_timestamp")
save_mode        = notebook_params.get("save_mode")
period_column    = notebook_params.get("period_column", "")

print(tgt_table)
print(target_tbl_prefix)
print(bronze_tbl_prefix)
print(silver_tbl_prefix)
print(gold_tbl_prefix)
print(match_keys)
print(load_date_column)
print(save_mode)
print(period_column)

# COMMAND ----------

FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "efront_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-efront/dev/files"
REMOTE_PIPELINE_FOLDER_NAME = "xio-efront/files"

def is_running_in_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

IS_DATABRICKS = is_running_in_databricks()
print(f">>> Running in Databricks:\n\t{IS_DATABRICKS}")

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

if IS_FEATURE:
    if IS_BUNDLE:
        PIPELINE_FOLDER_NAME = FEAT_BUNDLE_PIPELINE_FOLDER_NAME
    else:
        PIPELINE_FOLDER_NAME = FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME
else:
    PIPELINE_FOLDER_NAME = REMOTE_PIPELINE_FOLDER_NAME

SYS_BASE_PATH = curr_dir.split(f"/{PIPELINE_FOLDER_NAME}")[0]
print(f">>> Base path:\n\t{SYS_BASE_PATH}")

print(f">>> Importing pipeline config & models from:\n\t{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}")
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
from pipeline_parameters import DEVELOPMENT_PREFIX, SILVER_DATABASE

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

target_datapoints = [
    "Total Shareholder Return", 
    "Total Shareholder Return (Annualised)", 
    "Total NAV Return", 
    "Total NAV Return (Annualised)",
    "Annualised Dividend Paid", 
    "Weighted Average Discount Rate"
]

# Base Tables
df_track_record_fund = spark.table(f"{bronze_tbl_prefix}track_record_fund")

# Lookup Tables
# SAFEGUARD: dropDuplicates on the specific join key guarantees a strict 1:1 join
df_ref_track_record_fund_metric_visibility = spark.table("oegen_data_prod_prod.core_data_model.ref_track_record_metric_visibility").where((F.col("domain")=="fund") & (F.col("is_visible")=="true")).withColumnRenamed("metric", "raw_metric")

df_ref_metric_display_name_mapping = spark.table("oegen_data_prod_prod.core_data_model.ref_metric_display_name_mapping").withColumnRenamed("metric", "raw_metric")

df_metric_dictionary = (
    spark.table(f"{silver_tbl_prefix}track_record_metric_dictionary")
    .select("normalised_metric", "definition")
    .dropDuplicates(["normalised_metric"]) # Enforce 1:1 on join key to prevent row explosion
)

df_ef_funds = (
    spark.table(f"{bronze_tbl_prefix}fund")
    .select("Fund_IQid", "Fund", "Short_Name")
    .dropDuplicates(["Short_Name"])  # Enforce 1:1 on join key
)

df_mapped_funds = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_mapping_fund")
    .where(F.col("source_system_id") == "SRCE_SYST_1001")
    .select("source_fund_name", "source_fund_id", "cdm_fund_id")
    .dropDuplicates(["source_fund_name"]) # Enforce 1:1 on join key
)

df_cdm_fund_core = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_fund_dim_core")
    .where(F.col("END_AT").isNull())
    .select("fund_core_id", "primary_fund_name_id", "fund_start_date", "fund_end_date", "base_currency")
    .dropDuplicates(["fund_core_id"]) # Enforce 1:1 on join key
)

df_cdm_fund_names = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_fund_dim_names")
    .where(F.col("END_AT").isNull())
    .select("fund_name_id", "fund_display_name")
    .dropDuplicates(["fund_name_id"]) # Enforce 1:1 on join key
)

# Pre-join the CDM names to the CDM core
df_cdm_funds = (
    df_cdm_fund_core.join(
        F.broadcast(df_cdm_fund_names),
        F.col("primary_fund_name_id") == F.col("fund_name_id"), 
        "left"
    )
    .withColumnRenamed("fund_display_name", "cdm_fund")
    .withColumnRenamed("base_currency", "fund_currency")
    .drop("primary_fund_name_id", "fund_name_id")
    .dropDuplicates(["fund_core_id"]) # Final safeguard before main join
)

# COMMAND ----------

# DBTITLE 1,Prep datapoints export data
# Prep datapointsexport data
df_datapoints_raw = (
    spark.table(f"{bronze_tbl_prefix}datapointsexport")
    .select("ENTITY", "REPORTINGDATE", "DATAPOINT_NAME", "VALUENUM", "datalake_ingestion_timestamp")
    .where(
        (F.lower(F.col("CATEGORY_NAME")) == "fund metrics") & 
        (F.lower(F.col("ENTITY_TYPE")) == "fund") &
        (F.col("DATAPOINT_NAME").isin(target_datapoints))
    )
)

# Normalize the datapoint names (lowercase, replace spaces and brackets with underscores)
df_datapoints_norm = (
    df_datapoints_raw.withColumn(
        "raw_metric",
        F.lower(F.regexp_replace(F.col("DATAPOINT_NAME"), r"[\s\(\)]+", "_"))
    )
    .withColumn("raw_metric", F.regexp_replace(F.col("raw_metric"), r"_$", "")) # Strip trailing underscores
)

# Pivot the datapoints so the metrics become columns
df_dp_pivoted = (
    df_datapoints_norm.groupBy("ENTITY", "REPORTINGDATE", "datalake_ingestion_timestamp")
    .pivot("raw_metric")
    .agg(F.first("VALUENUM"))
    .withColumnRenamed("ENTITY", "ef_fund_id")
    .withColumnRenamed("REPORTINGDATE", "reporting_date")
)

# Map the pivoted datapoints to the dimension tables
df_dp_mapped = (
    df_dp_pivoted
    .join(F.broadcast(df_ef_funds), F.col("ef_fund_id") == F.col("Fund_IQid"), "left")
    .join(F.broadcast(df_mapped_funds), F.col("ef_fund_id") == F.col("source_fund_id"), "left")
    .join(F.broadcast(df_cdm_funds), F.col("cdm_fund_id") == F.col("fund_core_id"), "left")
    .withColumnRenamed("Short_Name", "ef_fund")
    .withColumnRenamed("datalake_ingestion_timestamp", "dp_ingest_ts")
).drop("source_fund_id", "fund_core_id", "source_fund_name", "Fund_IQid", "Fund")

display(df_dp_mapped)

# COMMAND ----------

# DBTITLE 1,Prep track record fund data
df_tr_mapped = (
    df_track_record_fund
    .join(F.broadcast(df_ef_funds), F.col("fund_short") == F.col("Short_Name"), "left")
    .join(F.broadcast(df_mapped_funds), F.col("Fund") == F.col("source_fund_name"), "left")
    .join(F.broadcast(df_cdm_funds), F.col("cdm_fund_id") == F.col("fund_core_id"), "left")
    .withColumnRenamed("source_fund_id", "ef_fund_id")
    .withColumnRenamed("FUND_SHORT", "ef_fund")
    .withColumnRenamed("datalake_ingestion_timestamp", "tr_ingest_ts")
).drop("Fund", "Short_Name", "source_fund_name", "fund_core_id", "Fund_IQid")

display(df_tr_mapped)

# COMMAND ----------

# A FULL OUTER JOIN ensures metrics from both tables merge into the exact same row.
join_keys = [
    "ef_fund_id", 
    "ef_fund", 
    "cdm_fund_id", 
    "cdm_fund",
    "fund_currency",
    "fund_start_date",
    "fund_end_date",
    "reporting_date"
]
# Temporarily rename the dimension columns in the Datapoints table
dim_cols = ["ef_fund", "cdm_fund_id", "cdm_fund", "fund_currency", "fund_start_date", "fund_end_date"]
for c in dim_cols:
    df_dp_mapped = df_dp_mapped.withColumnRenamed(c, f"dp_{c}")

# FULL OUTER JOIN purely on the true primary keys - guarantees the TR metrics and DP metrics merge onto the exact same row
join_keys = ["ef_fund_id", "reporting_date"]
df_joined = df_tr_mapped.join(df_dp_mapped, on=join_keys, how="full_outer")

# Coalesce the dimensions safely
# It uses the Track Record dimension, then gracefully falls back to the Datapoints dimension if TR is missing
for c in dim_cols:
    df_joined = df_joined.withColumn(c, F.coalesce(F.col(c), F.col(f"dp_{c}"))).drop(f"dp_{c}")

df = df_joined.toDF(*[c.lower() for c in df_joined.columns])

# COMMAND ----------

# DBTITLE 1,Formatting & column ordering
# Cast reporting date and compute flags
df = (
    df.withColumn("report_date", F.to_date(F.col("reporting_date")))
      .withColumn("fund_is_active", 
          F.when(
              (F.col("report_date") >= F.col("fund_start_date")) & 
              ((F.col("report_date") < F.col("fund_end_date")) | F.col("fund_end_date").isNull()), 
              True
          ).otherwise(False)
      )
      .withColumn("inception_date", F.to_date(F.col("fund_start_date")))
      .withColumn("datalake_ingestion_timestamp", F.coalesce(F.col("tr_ingest_ts"), F.col("dp_ingest_ts")))
      .drop("reporting_date", "fund_start_date", "fund_end_date", "tr_ingest_ts", "dp_ingest_ts")
)

all_cols = df.columns

explicit_cols = ["ef_fund_id", "ef_fund", "cdm_fund_id", "cdm_fund", "fund_currency", "fund_is_active", "inception_date", "report_date", "datalake_ingestion_timestamp"]
middle_cols = sorted([c for c in all_cols if c not in explicit_cols])

final_col_order = ["ef_fund_id", "ef_fund", "cdm_fund_id", "cdm_fund", "fund_currency", "fund_is_active", "inception_date", "report_date"] + middle_cols + ["datalake_ingestion_timestamp"]

df_unpivoted = (
    df.select(*final_col_order)
      .orderBy(F.col("report_date").desc(), F.col("ef_fund").asc())
      .withColumn("refresh_timestamp", F.current_timestamp())
)

display(df_unpivoted)

# COMMAND ----------

# -----------------------------------------------------------------------
# 1. Define Identifiers 
# -----------------------------------------------------------------------
id_vars = ["ef_fund_id", "ef_fund", "cdm_fund_id", "cdm_fund", "fund_currency", "inception_date"]

# -----------------------------------------------------------------------
# 2. Capture ALL dimensional data and 'fund_is_active' before unpivoting
# -----------------------------------------------------------------------
fund_window = W.Window.partitionBy("ef_fund_id").orderBy(F.col("report_date").desc())

# This lookup table permanently protects our IDs from getting filtered out later!
df_fund_dimensions = (
    df_unpivoted.withColumn("rn", F.row_number().over(fund_window))
                .filter(F.col("rn") == 1)
                .select(*id_vars, "fund_is_active")
)

# -----------------------------------------------------------------------
# 3. Unpivot ALL data 
# -----------------------------------------------------------------------
df_unpivoted = df_unpivoted.withColumn("report_date_str", F.date_format("report_date", "yyyy"))

exclude_cols = set(id_vars + ["fund_is_active", "report_date", "report_date_str", "datalake_ingestion_timestamp", "refresh_timestamp"])
metric_cols = [c for c in df_unpivoted.columns if c not in exclude_cols]

stack_args = ", ".join([f"'{c}', {c}" for c in metric_cols])
stack_expr = f"stack({len(metric_cols)}, {stack_args}) as (raw_metric, value)"

df_melted = df_unpivoted.select("ef_fund_id", "report_date", "report_date_str", F.expr(stack_expr))

# -----------------------------------------------------------------------
# 4. Find the LATEST NON-NULL date and value PER METRIC
# -----------------------------------------------------------------------
metric_window = W.Window.partitionBy("ef_fund_id", "raw_metric").orderBy(F.col("report_date").desc())

df_latest = (
    df_melted.filter(F.col("value").isNotNull()) 
             .withColumn("rn", F.row_number().over(metric_window))
             .filter(F.col("rn") == 1)
             .select(
                 "ef_fund_id",
                 "raw_metric",
                 F.col("report_date").alias("latest_metric_date"),
                 F.col("value").alias("latest_metric_value")
             )
)

# -----------------------------------------------------------------------
# 5. Pivot historical December 31st data
# -----------------------------------------------------------------------
df_pivoted = (
    df_melted.filter((F.month("report_date") == 12) & (F.dayofmonth("report_date") == 31))
             .groupBy("ef_fund_id", "raw_metric")
             .pivot("report_date_str")
             .agg(F.first("value"))
)

# -----------------------------------------------------------------------
# 6. Join everything together
# -----------------------------------------------------------------------
df_tr_dpe = (
    df_pivoted.join(df_latest, on=["ef_fund_id", "raw_metric"], how="outer")
              .join(df_fund_dimensions, on="ef_fund_id", how="left")
)

df_filtered_by_switch = (
    df_tr_dpe
    .join(
        df_ref_metric_display_name_mapping, 
        on="raw_metric", 
        how="left"
    )
    .withColumnRenamed("display_name", "metric")
    .join(
        df_metric_dictionary,
        F.col("raw_metric") == F.col("normalised_metric"),
        how="left"
    )
    .drop("normalised_metric") # Drop the extra key after joining
)

# We want: IDs -> metric -> definition -> latest_metric_date -> latest_metric_value -> 2021 -> 2022 -> etc.
# Note: df_pivoted still has the name "raw_metric", so we exclude that here
date_cols = sorted([c for c in df_pivoted.columns if c not in ["ef_fund_id", "raw_metric"]])

final_cols = id_vars + ["fund_is_active", "raw_metric", "metric", "definition", "latest_metric_date", "latest_metric_value"] + date_cols

df_final = df_filtered_by_switch.select(*final_cols).withColumn("refresh_timestamp", F.current_timestamp())

display(df_final)

# COMMAND ----------

print("Saving data to Delta...")

if save_mode == "append":
    append_to_delta(spark, df_final, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "overwrite":
    overwrite_to_delta(spark, df_final, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "merge":
    merge_into_delta(spark,
                     df=df_final,
                     target=f"{target_tbl_prefix}{tgt_table}",
                     match_keys=match_keys,
                     period_column=period_column)
else:
    raise Exception(f"Invalid save mode: {save_mode}")