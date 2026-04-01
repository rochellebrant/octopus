# Databricks notebook source
# MAGIC %md
# MAGIC # About
# MAGIC ## Purpose
# MAGIC - Create a single, trusted FX reference dataset that shows the exchange rate from each currency into GBP, by date.
# MAGIC ## Input
# MAGIC - Raw FX rates that may be stored in either direction (e.g. USD → GBP or GBP → USD).
# MAGIC - Currency names rather than standard currency codes.
# MAGIC ## Standardisation
# MAGIC - Convert currency names (e.g. “US Dollar”) into standard currency codes (USD, EUR, etc.).
# MAGIC - Ignore any FX records where the currency name isn’t recognised.
# MAGIC ## GBP normalisation
# MAGIC - Ensure all FX rates are expressed as “Rate to GBP”:
# MAGIC   - If the rate already goes into GBP → use it as-is.
# MAGIC   - If the rate goes out of GBP → invert it (1 ÷ rate).
# MAGIC - This guarantees one consistent definition of FX across the business.
# MAGIC ## Resulting output
# MAGIC - For each currency and reference date:
# MAGIC   - The value of 1 unit of that currency in GBP.
# MAGIC - Each record is timestamped so we know when it was last refreshed.
# MAGIC ## Storage behaviour
# MAGIC - The final FX table is either:
# MAGIC   - Fully replaced,
# MAGIC   - Appended to,
# MAGIC   - Or updated (merged),
# MAGIC - depending on how the business wants the FX history maintained.

# COMMAND ----------

# DBTITLE 1,Imports
import json
import sys
import os
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Set variables
dbutils.widgets.text("tgt_table", "")
dbutils.widgets.text("target_tbl_prefix", "")
dbutils.widgets.text("bronze_tbl_prefix", "")
dbutils.widgets.text("silver_tbl_prefix", "")
dbutils.widgets.text("gold_tbl_prefix", "")
dbutils.widgets.text("notebook_params", "")

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

# COMMAND ----------

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
from pipeline_parameters import DEVELOPMENT_PREFIX, SILVER_DATABASE

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

# DBTITLE 1,FX Currency
curr_map_data = [
    ("GBP", "British Pound"),
    ("EUR", "Euro"),
    ("USD", "US Dollar"),

    ("AUD", "Australian Dollar"),
    ("SEK", "Swedish Krona"),
    ("PLN", "Polish Zloty"),
    ("NOK", "Norwegian Kroner"),
    ("JPY", "Japanese Yen"),
    ("KRW", "South-Korean Won"),
    ("CAD", "Canadian Dollar"),
]

df_curr_name_map = spark.createDataFrame(curr_map_data, ["curr_code", "curr_name"])

df_fx_raw = (
    spark.table(f"{bronze_tbl_prefix}fx")
         .alias("fx")
         .join(
             df_curr_name_map.alias("m_src"),
             F.col("fx.Source_Currency") == F.col("m_src.curr_name"),
             "inner"
         )
         .join(
             df_curr_name_map.alias("m_dest"),
             F.col("fx.Destination_Curr") == F.col("m_dest.curr_name"),
             "inner"
         )
         .select(
             F.col("m_src.curr_code").alias("src_code"),
             F.col("m_dest.curr_code").alias("dest_code"),
             F.col("fx.Fx_Rate").alias("Fx_Rate"),
             F.col("fx.Ref_Date").alias("Ref_Date"),
            #  F.last_day("fx.Ref_Date").alias("Month_End")
         )
)
# display(df_fx_raw)


# Get Rate_to_Gbp for non-GBP currencies, directly and indirectly
df_fx_direct = (
    df_fx_raw
    .filter((F.col("dest_code") == "GBP") & (F.col("src_code") != "GBP"))
    .select(
        F.col("src_code").alias("Currency"),
        F.col("Ref_Date"),
        # F.col("Month_End"),
        F.col("Fx_Rate").alias("Rate_to_Gbp")
    )
)

df_fx_inverse = (
    df_fx_raw
    .filter((F.col("src_code") == "GBP") & (F.col("dest_code") != "GBP"))
    .select(
        F.col("dest_code").alias("Currency"),
        F.col("Ref_Date"),
        # F.col("Month_End"),        
        (F.lit(1.0) / F.col("Fx_Rate")).alias("Rate_to_Gbp")
    )
)

df_fx_vs_gbp = df_fx_direct.unionByName(df_fx_inverse)
# display(df_fx_vs_gbp)

df_fx_vs_gbp_all = (
    df_fx_vs_gbp
    .orderBy("Currency", "Ref_Date")
    .withColumn("refresh_timestamp", F.current_timestamp())
)

display(df_fx_vs_gbp_all)

# df_fx_vs_gbp_monthly = (
#     df_fx_vs_gbp
#     .groupBy("Currency", "Month_End")
#     .agg(
#         F.max("Rate_to_Gbp").alias("Rate_to_Gbp")
#     )
#     .orderBy("Month_End")
#     .withColumn("refresh_timestamp", F.current_timestamp())
# )
# display(df_fx_vs_gbp_monthly)

# COMMAND ----------

if save_mode == "append":
    append_to_delta(spark, df_fx_vs_gbp_all, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "overwrite":
    overwrite_to_delta(spark, df_fx_vs_gbp_all, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "merge":
    merge_into_delta(spark,
                            df=df_fx_vs_gbp_all,
                            target=f"{target_tbl_prefix}{tgt_table}",
                            match_keys=match_keys,
                            period_column=period_column)
else:
    raise Exception(f"Invalid save mode: {save_mode}")

# COMMAND ----------

