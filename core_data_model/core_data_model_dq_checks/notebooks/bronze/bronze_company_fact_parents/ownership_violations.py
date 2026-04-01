# Databricks notebook source
# # CONFIG & WIDGETS
# dbutils.widgets.text("bundle_target", "dev")
# dbutils.widgets.text("bundle_root_path", "")
# dbutils.widgets.text("source_catalog", "oegen_data_prod_prod")
# dbutils.widgets.text("source_schema", "core_data_model_dev")
# dbutils.widgets.text("dq_catalog", "oegen_data_prod_prod")
# dbutils.widgets.text("dq_schema", "core_data_model_dq_dev")
# dbutils.widgets.text("dq_tbl_prefix", "bronze")
# dbutils.widgets.text("dq_config_home", "core_data_model/core_data_model_dq_checks")
# dbutils.widgets.text("config_rel_path", "bronze")
# dbutils.widgets.text("COMMON_PATH", "")
# dbutils.widgets.text("table_include", "*") # future work

# COMMAND ----------

# DBTITLE 1,import funcs
import os
import sys

BUNDLE_TARGET = dbutils.widgets.get("bundle_target").strip()
COMMON_PATH = dbutils.widgets.get("common_path").strip()

if BUNDLE_TARGET == "dev":

    def find_repo_root(path, markers=['.github']):
        while path != os.path.dirname(path):
            if any(marker in os.listdir(path) for marker in markers):
                return path
            path = os.path.dirname(path)
        return None
    
    current_dir = os.getcwd()
    REPO_ROOT = find_repo_root(current_dir)

    if REPO_ROOT:
        if REPO_ROOT not in sys.path:
            sys.path.append(REPO_ROOT)

        FUNCS_PATH = os.path.join(REPO_ROOT, COMMON_PATH, "funcs")
        if COMMON_PATH not in sys.path and os.path.exists(FUNCS_PATH):
            sys.path.append(FUNCS_PATH)
            
        print(f"✅ Appended common libs from: {FUNCS_PATH}")
        
    else:
        raise FileNotFoundError("Could not find the repository root. Ensure databricks.yml exists at the root.")

else:
    REPO_ROOT = dbutils.widgets.get("bundle_root_path").strip()
    if COMMON_PATH and os.path.exists(COMMON_PATH):
        sys.path.append(COMMON_PATH)
        print(f"✅ Appended common libs from: {COMMON_PATH}")
    else:
        raise FileNotFoundError(f"Common libs path not found: {COMMON_PATH}")

# COMMAND ----------

# DBTITLE 1,default variables
table_name = dbutils.widgets.get("table_name")
check_name = dbutils.widgets.get("check_name")
issues_fqtn = dbutils.widgets.get("issues_fqtn")

source_db = f"{dbutils.widgets.get('source_catalog')}.{dbutils.widgets.get('source_schema')}"
source_fqtn = f"{source_db}.{table_name}"

print(f"Running bespoke check: {check_name} for table: {table_name}")
print(f"Saving issues to: {issues_fqtn}")

# COMMAND ----------

# DBTITLE 1,custom variables
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from TemporalToolkit import TemporalToolkit

bronze_company_fact_parents = spark.table(f"{source_fqtn}")
key_cols = ["company_core_id"]

# COMMAND ----------

# DBTITLE 1,define bespoke check  function (validate_segment_allocation)
def estimate_df_size_gb(df, sample_n=10000, seed=42):
    # Estimate avg row size from a sample
    sample = df.limit(sample_n)

    # Turn each row into JSON, then measure bytes
    avg_row_bytes = (
        sample
        .select(F.avg(F.length(F.to_json(F.struct(*[F.col(c) for c in df.columns])))).alias("avg_chars"))
        .collect()[0]["avg_chars"]
    )

    if avg_row_bytes is None:
        return 0.0

    # rough bytes ~= chars for ASCII-ish JSON; for safety you can multiply by ~1.1-1.5 if lots of unicode
    avg_row_bytes = float(avg_row_bytes)

    # 2) Multiply by row count
    n = df.count()

    est_bytes = avg_row_bytes * n
    return est_bytes / (1024**3)


def validate_segment_allocation(
    df: DataFrame,
    key_cols: list[str],
    seg_start: str,
    seg_end: str,
    value_col: str,
    max_threshold: float = 100.0,
    epsilon: float = 1e-6,
    out_sum_col: str = "total_allocation"
) -> tuple[DataFrame, DataFrame]:
    """
    Groups data by segment intervals and validates that the sum of a value 
    (e.g., ownership %) does not exceed a defined threshold.
    """
    
    # Define the grouping columns (Entity + Time Slice)
    grouping_cols = key_cols + [seg_start, seg_end]
    
    # Aggregate the totals
    # Rounding to 6 decimal places handles most floating-point jitter
    summed_df = (
        df.groupBy(*grouping_cols)
        .agg(
            F.round(F.sum(F.col(value_col)), 6).alias(out_sum_col),
            F.count("*").alias("record_count")
        )
    )
    
    # Identify segments where the threshold is violated
    # Logic: Sum > (100 + 0.000001)
    is_over_limit = F.col(out_sum_col) > (F.lit(max_threshold) + F.lit(epsilon))
    
    issues_df = summed_df.where(is_over_limit).withColumn(
        "violation_delta", F.col(out_sum_col) - F.lit(max_threshold)
    )
    
    return summed_df, issues_df

# COMMAND ----------

# DBTITLE 1,perform check - part 1
# Standardise
parents_clean = TemporalToolkit.standardize_intervals(
    df=bronze_company_fact_parents,
    start_col="active_from_date",
    end_col="active_to_date", 
    null_replacement="9999-12-31 23:59:59"
    ).withColumn("ownership_pct", F.col("ownership_percentage").cast("double")).where(F.col("END_AT").isNull())

# Slice
segments= TemporalToolkit.get_segments_from_single_table(
    parents_clean, key_cols, ["active_from_date", "active_to_date"]
)

broadcast_right = True if estimate_df_size_gb(parents_clean) < 1.0 else False
print(broadcast_right)

covered = TemporalToolkit.join_state_to_segments(
    segments_df=segments,
    windows_df=parents_clean,
    key_cols=key_cols,
    seg_start="seg_start",
    seg_end="seg_end",
    win_start="active_from_date",
    win_end="active_to_date",
    join_type="left",
    broadcast_right=broadcast_right
)
display(covered)

# COMMAND ----------

# DBTITLE 1,perform check - part 2
summed, issues_df = validate_segment_allocation(
    df=covered,
    key_cols=key_cols,
    seg_start="seg_start",
    seg_end="seg_end",
    value_col="ownership_pct",
    max_threshold=100.0,
    epsilon=1e-6,
    out_sum_col="total_ownership_pct"
)
display(summed.orderBy("company_core_id", "seg_start", "seg_end"))
display(issues_df)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

