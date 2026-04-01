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

# COMMAND ----------

# DBTITLE 1,default variable
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
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

bronze_company_fact_parents = spark.table(f"{source_fqtn}")
key_cols = ["company_core_id"]

# COMMAND ----------

# DBTITLE 1,define bespoke check  function (detect cycles)
def detect_cycles(
    parents_df, 
    child_col="company_core_id", 
    parent_col="parent_company_id", 
    max_depth=10
):
    # 1. Define explicit schema for the results
    schema = StructType([
        StructField("start_node", StringType(), True),
        StructField("end_node", StringType(), True),
        StructField("depth", IntegerType(), True),
        StructField("path", ArrayType(StringType()), True)
    ])

    # Pre-optimization: Leaf Trimming
    # A cycle requires a node to be both a parent and a child.
    nodes_with_children = parents_df.select(F.col(child_col).alias("node_id")).distinct()
    nodes_with_parents = parents_df.select(F.col(parent_col).alias("node_id")).distinct()
    
    # Potential candidates for cycles
    potential_nodes = nodes_with_children.join(nodes_with_parents, on="node_id", how="inner")

    edges = parents_df.select(
        F.col(child_col).alias("src"),
        F.col(parent_col).alias("dst")
    ).join(potential_nodes, F.col("src") == F.col("node_id"), "inner") \
     .select("src", "dst") \
     .distinct()

    # Initialize an empty results DF with our schema
    cycles_found = parents_df.sparkSession.createDataFrame([], schema)

    # Initialize Paths
    paths = edges.select(
        F.col("src").alias("start_node"),
        F.col("dst").alias("next_node"),
        F.array(F.col("src"), F.col("dst")).alias("path"),
        F.lit(1).alias("depth")
    )

    for d in range(1, max_depth + 1):
        # Detect closed loops
        new_cycles = paths.filter(F.col("start_node") == F.col("next_node")) \
            .select(
                F.col("start_node"), 
                F.col("next_node").alias("end_node"), 
                F.col("depth"), 
                F.col("path")
            )

        # Union new cycles into our results
        cycles_found = cycles_found.unionByName(new_cycles)

        if d == max_depth:
            break
            
        # Join for next hop - only keep paths that haven't finished yet
        paths = (
            paths.filter(F.col("start_node") != F.col("next_node")).alias("p")
            .join(F.broadcast(edges).alias("e"), F.col("p.next_node") == F.col("e.src"))
            .where(~F.array_contains(F.col("p.path"), F.col("e.dst")) | (F.col("e.dst") == F.col("p.start_node")))
            .select(
                F.col("p.start_node"),
                F.col("e.dst").alias("next_node"),
                F.concat(F.col("p.path"), F.array(F.col("e.dst"))).alias("path"),
                (F.col("p.depth") + 1).alias("depth")
            )
        )

        # Truncate lineage to keep Serverless memory usage low
        if d % 2 == 0:
            paths = paths.localCheckpoint() 

        # Early exit if no more paths to explore
        if not paths.limit(1).count():
            break

    # Return deduped cycles
    return cycles_found.dropDuplicates(["path"])

# COMMAND ----------

# DBTITLE 1,run tests on function
def test_detect_cycles_logic(spark):
    # --- SCENARIO 1: Simple Cycle (A -> B -> A) ---
    # --- SCENARIO 2: Linear Path (C -> D -> E) [Should be filtered by leaf trimming] ---
    # --- SCENARIO 3: Self-Parent (F -> F) ---
    test_data = [
        Row(company_core_id="A", parent_company_id="B"),
        Row(company_core_id="B", parent_company_id="A"),
        Row(company_core_id="C", parent_company_id="D"),
        Row(company_core_id="D", parent_company_id="E"),
        Row(company_core_id="F", parent_company_id="F")
    ]
    df = spark.createDataFrame(test_data)
    
    results = detect_cycles(df, max_depth=5)
    collected = results.collect()
    
    # Assertions
    # 1. We expect 2 unique cycles (A-B-A and F-F)
    # 2. C-D-E should be ignored because E is not a child (leaf trimming)
    assert len(collected) >= 2, f"Expected at least 2 cycles, found {len(collected)}"
    
    paths = [set(row["path"]) for row in collected]
    assert {"A", "B"}.issubset(set().union(*paths)), "Failed to detect A-B cycle"
    assert {"F"}.issubset(set().union(*paths)), "Failed to detect self-parent cycle"
    
    # Ensure depth is recorded correctly
    for row in collected:
        assert len(row["path"]) == row["depth"] + 1

def test_detect_cycles_max_depth(spark):
    # --- SCENARIO: A 3-node loop (A -> B -> C -> A) with max_depth=1 ---
    test_data = [
        Row(company_core_id="A", parent_company_id="B"),
        Row(company_core_id="B", parent_company_id="C"),
        Row(company_core_id="C", parent_company_id="A")
    ]
    df = spark.createDataFrame(test_data)
    
    # With max_depth=1, it shouldn't find a 3-node cycle
    results = detect_cycles(df, max_depth=1)
    assert results.count() == 0, "Found a cycle deeper than max_depth"

def test_detect_cycles_empty_and_null(spark):
    # --- SCENARIO: Nulls and Empty DF ---
    schema = "company_core_id STRING, parent_company_id STRING"
    test_data = [Row(company_core_id="A", parent_company_id=None)]
    df = spark.createDataFrame(test_data, schema)
    
    results = detect_cycles(df)
    assert results.count() == 0, "Should not find cycles in null data"
    assert "start_node" in results.columns

# Execution
print("Testing Cycle Detection Logic...")
test_detect_cycles_logic(spark)
print("Testing Max Depth Constraint...")
test_detect_cycles_max_depth(spark)
print("Testing Null/Empty Safety...")
test_detect_cycles_empty_and_null(spark)
print("✅ Graph tests passed!")

# COMMAND ----------

# DBTITLE 1,perform check
issues_df = detect_cycles(
    bronze_company_fact_parents, 
    child_col="company_core_id", 
    parent_col="parent_company_id", 
    max_depth=20,
)
display(issues_df)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

