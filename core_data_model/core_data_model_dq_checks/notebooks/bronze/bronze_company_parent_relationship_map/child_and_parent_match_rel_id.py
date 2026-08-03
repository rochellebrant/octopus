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
from pyspark.sql import functions as F
import os

bronze_company_parent_relationship_map = spark.table(f"{source_fqtn}")

# COMMAND ----------

# DBTITLE 1,define bespoke check  function (check_self_parenting)
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# --- 1. The Corrected Logic ---
def check_endpoints_match(df):
    """
    Checks if the start/end of rel_id match ultimate_parent_id and ultimate_child_id.
    Logic: First Node = Parent, Last Node = Child
    """
    split_col = F.split(F.col("rel_id"), "\\.")
    first_node = F.element_at(split_col, 1)
    last_node  = F.element_at(split_col, -1)
    
    return df.withColumn("is_valid_endpoints", 
        F.when(F.col("rel_id").isNull(), F.lit(False))
         .when(
            (F.trim(first_node) == F.trim(F.col("ultimate_parent_id"))) & 
            (F.trim(last_node)  == F.trim(F.col("ultimate_child_id"))),
            F.lit(True)
         ).otherwise(F.lit(False))
    )

# --- 2. The Test Suite ---
class TestEndpointValidation(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Databricks Session Fix
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            cls.spark = SparkSession.builder \
                .master("local[1]") \
                .appName("Endpoint_Tests") \
                .getOrCreate()

    def test_endpoints(self):
        # Schema: rel_id, ultimate_parent_id, ultimate_child_id, expected_result
        data = [
            # --- HAPPY PATHS (Parent -> Child) ---
            # rel_id = "P.C", Parent="P", Child="C" -> True
            ("Correct Simple", "PARENT.CHILD", "PARENT", "CHILD", True),
            
            # rel_id = "P.X_X.C", Parent="P", Child="C" -> True
            ("Correct Chain",  "TOP.MID_MID.BOTTOM", "TOP", "BOTTOM", True),
            
            # --- FAILURE PATHS ---
            # Logic Swapped (User provided Child as Parent)
            ("Swapped inputs", "PARENT.CHILD", "CHILD", "PARENT", False),
            
            # Wrong ID completely
            ("Wrong Parent",   "PARENT.CHILD", "WRONG_P", "CHILD", False),
            ("Wrong Child",    "PARENT.CHILD", "PARENT", "WRONG_C", False),
            
            # Nulls
            ("Null rel_id",    None, "PARENT", "CHILD", False),
        ]

        schema = StructType([
            StructField("desc", StringType(), True),
            StructField("rel_id", StringType(), True),
            StructField("ultimate_parent_id", StringType(), True),
            StructField("ultimate_child_id", StringType(), True),
            StructField("expected", BooleanType(), True)
        ])
        
        df_input = self.spark.createDataFrame(data, schema)
        df_result = check_endpoints_match(df_input)

        results = df_result.select("desc", "rel_id", "ultimate_parent_id", "ultimate_child_id", "expected", "is_valid_endpoints").collect()

        print(f"\n{'Description':<15} | {'Rel ID':<20} | {'Parent':<10} | {'Child':<10} | {'Exp':<5} | {'Act':<5} | {'Status'}")
        print("-" * 100)
        
        for row in results:
            status = "PASS" if row["expected"] == row["is_valid_endpoints"] else "FAIL"
            print(f"{row['desc']:<15} | {str(row['rel_id']):<20} | {row['ultimate_parent_id']:<10} | {row['ultimate_child_id']:<10} | {str(row['expected']):<5} | {str(row['is_valid_endpoints']):<5} | {status}")
            self.assertEqual(row["is_valid_endpoints"], row["expected"], f"Failed on: {row['desc']}")

# --- 3. Execution ---
suite = unittest.TestLoader().loadTestsFromTestCase(TestEndpointValidation)
unittest.TextTestRunner(verbosity=2).run(suite)

# COMMAND ----------

# DBTITLE 1,run tests on function
issues_df  = check_endpoints_match(bronze_company_parent_relationship_map).where(F.col("is_valid_endpoints") != 'true')
display(issues_df)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

