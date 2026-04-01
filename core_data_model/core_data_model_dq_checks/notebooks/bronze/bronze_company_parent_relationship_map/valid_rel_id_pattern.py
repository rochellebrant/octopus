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
from pyspark.sql import functions as F
import os

bronze_company_parent_relationship_map = spark.table(f"{source_fqtn}")

# COMMAND ----------

# DBTITLE 1,define bespoke check  function (check_self_parenting)
from pyspark.sql.types import BooleanType

def check_rel_id_pattern(df):
    
    # 1. Split the string by the dot literal
    # Note: We escape dot as \\. because split takes a regex
    split_col = F.split(F.col("rel_id"), "\\.")
    
    # 2. Define the logic for the "Mirror Check" (e.g., is "X_X" valid?)
    # We use 'x' as the lambda variable representing a string in the middle of the array
    str_len = F.length(df.rel_id) # Placeholder, actual usage below uses lambda x
    
    # Logic to apply to every middle element:
    # 1. Length must be odd (to have a perfect center '_')
    # 2. Middle char must be '_'
    # 3. Left side must equal Right side
    
    # Using Higher Order Functions (Spark 3.0+)
    # We transform the split array to remove the first and last elements, keeping only the "junctions"
    # slice(arr, start, length). indices are 1-based.
    # We want from index 2 up to size-1.
    
    middle_elements = F.expr("slice(split_ids, 2, size(split_ids)-2)")
    
    # Define the validation expression for a single string 'x'
    # Use SQL expression syntax for cleaner implementation inside 'forall'
    # x is the string, e.g. "COMP_1002_COMP_1002"
    # mid_index = (length(x) + 1) / 2
    validation_expr = """
        length(x) % 2 = 1 
        AND substring(x, cast((length(x)+1)/2 as int), 1) = '_'
        AND substring(x, 1, cast((length(x)-1)/2 as int)) = 
            substring(x, cast((length(x)+3)/2 as int), cast((length(x)-1)/2 as int))
    """

    # 3. Apply logic
    return df.withColumn("split_ids", split_col) \
             .withColumn("is_valid_pattern", 
                F.when(F.size(F.col("split_ids")) < 2, F.lit(False)) # Must have at least A.B
                 .when(F.size(F.col("split_ids")) == 2, F.lit(True)) # A.B is always structurally valid if it split into 2
                 .otherwise(
                     # Check all middle elements follow the X_X pattern
                     F.expr(f"forall(slice(split_ids, 2, size(split_ids)-2), x -> {validation_expr})")
                 )
             ).drop("split_ids") # Clean up temp column

result_df = check_rel_id_pattern(bronze_company_parent_relationship_map)
issues_df = result_df.filter("is_valid_pattern = False")
display(issues_df)

# COMMAND ----------

# DBTITLE 1,run tests on function
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# --- 1. The Logic Under Test ---
def add_rel_id_validation(df):
    """
    Checks if rel_id follows the correct 'A.B_B.C' pattern.
    """
    split_col = F.split(F.col("rel_id"), "\\.")
    
    # Logic: 
    # 1. Length of middle segment must be odd (Left + '_' + Right)
    # 2. Middle char must be '_'
    # 3. Left side must equal Right side
    validation_expr = """
        length(x) % 2 = 1 
        AND substring(x, cast((length(x)+1)/2 as int), 1) = '_'
        AND substring(x, 1, cast((length(x)-1)/2 as int)) = 
            substring(x, cast((length(x)+3)/2 as int), cast((length(x)-1)/2 as int))
    """

    return df.withColumn("split_ids", split_col) \
             .withColumn("is_valid_pattern", 
                F.when(F.col("rel_id").isNull(), F.lit(False))
                 .when(F.size(F.col("split_ids")) < 2, F.lit(False)) # Fail if no dot (size 1)
                 .when(F.size(F.col("split_ids")) == 2, F.lit(True)) # Pass if exactly A.B
                 .otherwise(
                     # Check all middle elements follow the X_X pattern
                     F.expr(f"forall(slice(split_ids, 2, size(split_ids)-2), x -> {validation_expr})")
                 )
             ).drop("split_ids")

# --- 2. The Test Suite ---
class TestRelIdPatterns(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # FIX: Check for existing Databricks session first
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            cls.spark = SparkSession.builder \
                .master("local[1]") \
                .appName("RelID_UnitTests") \
                .getOrCreate()

    def test_rel_id_patterns(self):
        # Test Data
        data = [
            ("Simple Pair", "COMP_1.COMP_2", True),
            ("Long Chain", "COMP_1.COMP_2_COMP_2.COMP_3", True),
            ("Very Long Chain", "A.B_B.C_C.D_D.E", True),
            ("Complex IDs", "US-100.UK-200_UK-200.JP-300", True),
            
            # Failures
            ("Single Node", "COMP_1", False),
            ("Broken Chain", "COMP_1.COMP_2_COMP_3.COMP_4", False),
            ("Bad Separator", "COMP_1.COMP_2-COMP_2.COMP_3", False), 
            ("Empty String", "", False),
            ("Null Value", None, False),
        ]

        schema = StructType([
            StructField("description", StringType(), True),
            StructField("rel_id", StringType(), True),
            StructField("expected_result", BooleanType(), True)
        ])
        
        # Create DataFrame
        df_input = self.spark.createDataFrame(data, schema)

        # Run Logic
        df_result = add_rel_id_validation(df_input)

        # Collect Results
        results = df_result.select("description", "rel_id", "expected_result", "is_valid_pattern").collect()

        # Assertions
        print(f"\n{'Description':<25} | {'Rel ID':<30} | {'Exp':<5} | {'Act':<5} | {'Status'}")
        print("-" * 95)
        
        for row in results:
            desc = row["description"]
            rel_id = str(row["rel_id"])
            expected = row["expected_result"]
            actual = row["is_valid_pattern"]
            
            status = "PASS" if expected == actual else "FAIL"
            print(f"{desc:<25} | {rel_id[:30]:<30} | {str(expected):<5} | {str(actual):<5} | {status}")
            
            self.assertEqual(actual, expected, f"Failed on: {desc}")

# --- 3. Execution ---
# 'argv' arg prevents unittest from parsing notebook arguments
suite = unittest.TestLoader().loadTestsFromTestCase(TestRelIdPatterns)
unittest.TextTestRunner(verbosity=2).run(suite)

# COMMAND ----------

# DBTITLE 1,perform check
if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

