# Databricks notebook source
# MAGIC %md
# MAGIC https://octoenergy.sharepoint.com/:x:/r/sites/Or-Data/_layouts/15/Doc.aspx?sourcedoc=%7B1E8FED42-8D47-45D8-8A01-F7CE412F1136%7D&file=Historic_TR_DL.xlsx&action=default&mobileredirect=true

# COMMAND ----------

import sys
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, date_format, trim

# COMMAND ----------

FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "efront_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-efront/dev/files"
REMOTE_PIPELINE_FOLDER_NAME = "xio-efront/files"

# COMMAND ----------

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
from pipeline_parameters import DEVELOPMENT_PREFIX, SOURCE_DATABASE

# COMMAND ----------

PREFIX = "" if DEVELOPMENT_PREFIX == "production" else DEVELOPMENT_PREFIX

prefix_message = "(Blank for production mode)" if PREFIX == "" else PREFIX
print(f"{'>>> PRODUCTION MODE ⚠️' if PREFIX == '' else '>>> Dev mode 👷🏼‍♀️🛠️'}")
print(f">>> PREFIX: {prefix_message}")
print(f">>> TARGET DATABASE: {SOURCE_DATABASE}")

# COMMAND ----------

def normalise_spaces(c: str) -> str:
    return c.strip().replace(" ", "_")

# COMMAND ----------

# DBTITLE 1,Cell 1
table = "track_record_asset_historics"

path = "/Volumes/oegen_data_prod_source/src_blackrock_efront_xio/landing_investments_efront/Historic_TR_Asset_DL.csv"

cols_to_select = ["ANNUALISED_YIELD", "ANNUAL_YIELD", "FUND_SHORT", "IRR_FROM_TODAY", "LEVERAGE", "LONG_TERM_IRR", "MOIC", "NAV", "PORTFOLIO_NAME", "QUARTERLY_YIELD", "REPORTING_DATE", "UNREALISED_NET_IRR"]

df_raw = (spark.read
      .option("header", True)
      .option("multiLine", False)     # if there are line breaks in cells
      .option("quote", '"')
      .option("escape", '"')
      .csv(path)
      )

# Filter out rows where a mandatory column is null or empty
# If a row doesn't have a FUND_SHORT, it's not a valid record
df_raw = df_raw.filter(
    col("FUND_SHORT").isNotNull() & 
    (trim(col("FUND_SHORT")) != "")
)

df_clean_cols = df_raw.select(
    *[
        col(c).alias(normalise_spaces(c))
        for c in df_raw.columns
    ]
)

df_clean_cols = df_clean_cols.withColumn(
    "REPORTING_DATE", 
    date_format(to_timestamp(col("REPORTING_DATE"), "M/d/yyyy"), "MM/dd/yyyy hh:mm:ss a")
)

df_final = df_clean_cols.select(*cols_to_select).withColumn("datalake_ingestion_timestamp", current_timestamp())

display(df_final)

# COMMAND ----------

SAVE_TO = f"{SOURCE_DATABASE}.{PREFIX}{table}"
spark.sql(f"DROP TABLE IF EXISTS {SAVE_TO}")

(df_final.write
   .mode("overwrite")
   .format("delta")
   .option("overwriteSchema", "true")
   .saveAsTable(SAVE_TO))

print(f"Saved to: {SAVE_TO}")

# COMMAND ----------

