# Databricks notebook source
import json 
from pyspark.sql import functions as F
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

# Prep datapointsexport data
df_datapoints_raw = (
    spark.table(f"{bronze_tbl_prefix}datapointsexport")
    .select("DATAPOINT_NAME")
    .where(
        (F.lower(F.col("CATEGORY_NAME")) == "fund metrics") & 
        (F.lower(F.col("ENTITY_TYPE")) == "fund")
    )
).distinct()

df_datapoints_metrics = (
    df_datapoints_raw.withColumn(
        "metric",
        F.lower(F.regexp_replace(F.col("DATAPOINT_NAME"), r"[\s\(\)]+", "_"))
    )
    .withColumn("metric", F.regexp_replace(F.col("metric"), r"_$", "")) # Strip trailing underscores
    .select(
        F.lit("fund").alias("domain"),
        F.lit("SRCE_SYST_1001").alias("source_system_id"),
        F.lit("datapointsexport").alias("dataset_name"),
        "metric"
    )
)

df_metric_display_names = (
    spark.table("oegen_data_prod_prod.core_data_model_dev.ref_metric_display_name_mapping").withColumnRenamed("metric", "normalised_metric")
)

display(df_metric_display_names)

# COMMAND ----------

non_metric_cols = ["FUND_SHORT", "REPORTING_DATE", "datalake_ingestion_timestamp"]
tr_metric_cols_list = spark.table(f"{bronze_tbl_prefix}track_record_fund").drop(*non_metric_cols).columns

metrics_data = [(m.lower(),) for m in tr_metric_cols_list]

df_tr_metrics = (
    spark.createDataFrame(metrics_data, schema=["metric"])
    .select(
        F.lit("fund").alias("domain"),
        F.lit("SRCE_SYST_1001").alias("source_system_id"),
        F.lit("track_record_fund").alias("dataset_name"),
        "metric"
    )
)

display(df_tr_metrics)

# COMMAND ----------

# Define the dictionary of metric definitions
metric_definitions = {
    "annual_net_irr": "The Internal Rate of Return calculated over a specific 1-year trailing period (or annualised over the life of the fund), net of management fees, expenses, and carried interest.",
    "annual_yield": "The total income (dividends/interest) actually generated and paid out by the investment over a trailing 12-month period, expressed as a percentage of the fund's NAV or invested capital.",
    "annualised_yield": "Total cash distributions to investors divided by the value of drawdowns since inception converted into an annual rate.",
    "net_cagr": "Compound Annual Growth Rate. The smoothed, annualised rate of return of an investment over a specified period longer than one year, net of fees.",
    "dpi": "Distributions to Paid-In Capital - Ratio of total distributions over contributed capital since inception.",
    "annualised_dividend_paid": "A forward-looking run-rate extrapolating a specific dividend amount for a quarter or month to estimate the full 12-month period payout.",
    "moic": "Multiple on Invested Capital - Ratio of the total value (realised plus unrealised) to the total invested capital.",
    "nav_yield": "The total income distributions divided by the fund's current Net Asset Value.",
    "quarterly_yield": "The income generated by the fund over a single quarter, expressed as a percentage of NAV or invested capital.",
    "target_net_irr": "The desired or expected Internal Rate of Return for a fund or investment, net of fees.",
    "total_nav_return": "The cumulative (non-annualised) return based on the growth of the Net Asset Value plus any distributions, from inception to date.",
    "total_nav_return_annualised": "Annualised growth using the Net Asset Value, accounting for distributions.",
    "net_total_return": "The investor's cumulative economic return from the beginning of the fund through capital growth and reinvested income.",
    "total_shareholder_return": "The cumulative (non-annualised) return experienced by an investor, reflecting changes in the publicly traded share price plus any dividends received.",
    "total_shareholder_return_annualised": "Annualised rate of return reflecting changes in share price plus dividends.",
    "unrealised_gross_irr": "The Internal Rate of Return calculated strictly on the active, unsold portion of the portfolio, before deducting fund-level fees and carry.",
    "unrealised_net_irr": "The Internal Rate of Return on the active, unsold portfolio, calculated after deducting estimated fund-level fees and accrued carried interest.",
    "weighted_average_discount_rate": "The average discount rate used in Discounted Cash Flow (DCF) models to value the fund's unlisted assets, weighted by proportional value.",
    "weighted_yield_units": "The overall yield of a portfolio calculated by taking the individual yield of each underlying asset and weighting it by its proportional sise."
}

# Convert dictionary to DataFrame
definitions_data = [(k, v) for k, v in metric_definitions.items()]
df_definitions = spark.createDataFrame(definitions_data, schema=["normalised_metric_def", "definition"])

# Update df_final to join the definitions and sequence the columns
df_final = (
    df_datapoints_metrics.union(df_tr_metrics)
    .join(
        df_metric_display_names, 
        on=[F.col("metric") == F.col("normalised_metric")], 
        how="left"
    )
    .drop("normalised_metric")
    .withColumnRenamed("metric", "normalised_metric")
    .join(
        df_definitions,
        on=[F.col("normalised_metric") == F.col("normalised_metric_def")],
        how="left"
    )
    .drop("normalised_metric_def")
    .withColumnRenamed("display_name", "metric")
    .withColumn("refresh_timestamp", F.current_timestamp())
    # Reorder to ensure definition is right before refresh_timestamp
    .select(
        "domain", 
        "source_system_id", 
        "dataset_name", 
        "normalised_metric", 
        "metric", 
        "definition", 
        "refresh_timestamp"
    )
    .orderBy(F.col("metric"))
)

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