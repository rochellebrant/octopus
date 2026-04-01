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

# DBTITLE 1,custom variables
import os
from pyspark.sql import functions as F

bronze_company_fact_parents = spark.table(f"{source_fqtn}")
key_cols = ["company_core_id"]

# COMMAND ----------

# DBTITLE 1,define bespoke check  function (check_self_parenting)
def check_self_parenting(parents_df, child_col="company_core_id", parent_col="parent_company_id"):
    """
    Returns rows where child == parent (self-parenting).
    """
    return (
        parents_df
        .where(F.col(child_col).isNotNull() & (F.col(child_col) == F.col(parent_col)))
        .select(child_col, parent_col, *[c for c in parents_df.columns if c not in [child_col, parent_col]])
    )

# COMMAND ----------

# DBTITLE 1,run tests on function
import pytest
from pyspark.sql import Row

def test_check_self_parenting(spark):
    # --- SCENARIO 1: Mixed Data ---
    # 101: Self-parenting (Should be caught)
    # 102: Normal parenting (Should be ignored)
    # 103: Null parent (Should be ignored)
    # 104: Parent is null string/empty (Should be ignored)
    test_data = [
        Row(company_core_id="101", parent_company_id="101", other_col="val1"),
        Row(company_core_id="102", parent_company_id="999", other_col="val2"),
        Row(company_core_id="103", parent_company_id=None,  other_col="val3"),
        Row(company_core_id="104", parent_company_id="",    other_col="val4"),
    ]
    df = spark.createDataFrame(test_data)
    
    # Run function
    results = check_self_parenting(df, "company_core_id", "parent_company_id")
    
    # Collect results for assertion
    collected = results.collect()
    
    # Assertions
    assert len(collected) == 1, f"Expected 1 issue, found {len(collected)}"
    assert collected[0]["company_core_id"] == "101", "Failed to catch self-parenting ID 101"
    assert "other_col" in results.columns, "Function dropped extra columns"

def test_check_self_parenting_empty(spark):
    # --- SCENARIO 2: Empty DataFrame ---
    schema = "company_core_id STRING, parent_company_id STRING, other_col STRING"
    empty_df = spark.createDataFrame([], schema)
    
    results = check_self_parenting(empty_df)
    
    assert results.count() == 0
    assert len(results.columns) == 3

# Execute tests
print("Running Success Case Test...")
test_check_self_parenting(spark)
print("Running Empty Case Test...")
test_check_self_parenting_empty(spark)
print("✅ All tests passed!")

# COMMAND ----------

# DBTITLE 1,perform check
issues_df = check_self_parenting(bronze_company_fact_parents, "company_core_id", "parent_company_id")
display(issues_df)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")