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
# dbutils.widgets.text("common_libs_path", "")
# dbutils.widgets.text("table_include", "*") # future work

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

# DBTITLE 1,custome variables
import os
from pyspark.sql import functions as F

silver_asset_lifecycle_fact = spark.table(f"{source_fqtn}")

asset_col = "asset_id" 
date_col = "event_date"
type_col = "lifecycle_milestone_type_id"
phase_col = "lifecycle_phase_id"
milestone_col = "lifecycle_milestone_id"

# COMMAND ----------

# Group by the collision keys: Asset + Date + Type
issues_df = (
    silver_asset_lifecycle_fact
    .groupBy(asset_col, date_col, type_col)
    .agg(
        # Count how many DIFFERENT phases exist at this exact timestamp/type
        F.countDistinct(phase_col).alias("distinct_phase_count"),
        
        # Collect them to see WHICH phases are fighting
        F.collect_set(phase_col).alias("conflicting_phases"),
        
        # Collect the milestones causing the conflict for debugging
        F.collect_list(
            F.struct(
                F.col(milestone_col).alias("milestone"),
                F.col(phase_col).alias("phase")
            )
        ).alias("milestone_details")
    )
    # Filter: We only care if more than 1 distinct phase claims this timestamp
    .filter(F.col("distinct_phase_count") > 1)
    .orderBy(asset_col, date_col)
)

print(f"Found {issues_df.count()} collisions where different phases start on the same date/type.")
display(issues_df)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

