# Databricks notebook source
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
from pyspark.sql import functions as f
from functools import reduce
import operator
import os

def check_misjoins(df):
    filter_condition = [
        f.col(col).isNotNull()
        for col in df.columns
        if col.startswith("misjoin")
    ]
    if filter_condition:
        combined = reduce(operator.or_, filter_condition)
        df.filter(combined).display()

# COMMAND ----------

# =================================================================================
# 1. ASSETS & ASSET MAPPING
# Naming convention: asset_dim__*, asst_invp_map__*
# =================================================================================

bronze_asset_infra_dim_core_df = (
    spark.table(f"{source_db}.bronze_asset_infra_dim_core")
    .filter("END_AT is null")
    .selectExpr("company_core_id AS asset_dim__company_id", "asset_infra_id AS asset_dim__asset_id")
)

bronze_asset_platform_dim_core_df = (
    spark.table(f"{source_db}.bronze_asset_platform_dim_core")
    .filter("END_AT is null")
    .selectExpr("company_core_id AS asset_dim__company_id", "asset_plat_id AS asset_dim__asset_id")
)

bronze_assets_df = bronze_asset_platform_dim_core_df.union(bronze_asset_infra_dim_core_df)

bronze_asset_invp_df = (
    spark.table(f"{source_db}.bronze_asset_invp")
    .selectExpr(
        "asset_id AS asst_invp_map__asset_id", 
        "invp_version_id AS asst_invp_map__version_id", 
        "investment_portfolio_id AS asst_invp_map__invp_id"
    )
)

# Join 1: Asset to Mapping
multi_join_step_one = bronze_assets_df.join(
    bronze_asset_invp_df,
    bronze_assets_df.asset_dim__asset_id == bronze_asset_invp_df.asst_invp_map__asset_id,
    "fullouter",
).withColumn(
    "misjoin_asset_to_map",
    f.when(f.col("asst_invp_map__asset_id").isNull(), "Asset exists in Dim but has no Invp Mapping")
     .when(f.col("asset_dim__asset_id").isNull(), "Asset-Invp Map references an Asset ID missing from Asset Dim")
     .otherwise(f.lit(None))
)


# =================================================================================
# 2. INVESTMENT PORTFOLIO INTERNAL (DIM <-> BRIDGE <-> FACT)
# Naming convention: invp_dim__*, invp_brg__*, invp_vers_*
# =================================================================================

bronze_invp_dim__af_df = (
    spark.table(f"{source_db}.bronze_investment_portfolio_dim_core_af")
    .filter("END_AT is null")
    .selectExpr("investment_portfolio_id AS invp_dim__invp_id", "invp_fund_core_id AS invp_dim__fund_id")
)

bronze_invp_bridge_af_df = (
    spark.table(f"{source_db}.bronze_investment_portfolio_bridge")
    .filter("END_AT is null")
    .selectExpr("investment_portfolio_id AS invp_brg__invp_id", "invp_version_id AS invp_brg__version_id")
)

bronze_invp_fact_ver_af_df = (
    spark.table(f"{source_db}.bronze_investment_portfolio_fact_version")
    .filter("END_AT is null")
    .selectExpr("invp_version_id AS invp_vers_version_id")
)

# Internal Invp Join (FULL OUTER to catch orphans in Bridge or Fact)
invp_canonical_df = (
    bronze_invp_dim__af_df.join(
        bronze_invp_bridge_af_df,
        bronze_invp_dim__af_df.invp_dim__invp_id == bronze_invp_bridge_af_df.invp_brg__invp_id,
        "fullouter"
    ).join(
        bronze_invp_fact_ver_af_df,
        bronze_invp_bridge_af_df.invp_brg__version_id == bronze_invp_fact_ver_af_df.invp_vers_version_id,
        "fullouter"
    )
).withColumn(
    "misjoin_invp_internal",
    f.when(f.col("invp_dim__invp_id").isNull() & f.col("invp_brg__invp_id").isNotNull(), "Invp Bridge has Invp ID missing from Invp Dim")
     .when(f.col("invp_brg__invp_id").isNull() & f.col("invp_dim__invp_id").isNotNull(), "Invp Dim has Invp ID missing from Invp Bridge")
     .when(f.col("invp_brg__version_id").isNull() & f.col("invp_vers_version_id").isNotNull(), "Invp Version Fact has Version ID missing from Invp Bridge")
     .when(f.col("invp_brg__version_id").isNotNull() & f.col("invp_vers_version_id").isNull(), "Invp Bridge has Invp Version ID missing from Invp Version Fact")
     .otherwise(f.lit(None))
)


# =================================================================================
# 3. ASSET MAP TO INVP CANONICAL
# =================================================================================

# We use coalesce in the join condition because full outer joins in step 2 might leave some IDs null
coalesced_invp_id = f.coalesce(f.col("invp_dim__invp_id"), f.col("invp_brg__invp_id"))
coalesced_version_id = f.coalesce(f.col("invp_vers_version_id"), f.col("invp_brg__version_id"))

multi_join_step_two = multi_join_step_one.join(
    invp_canonical_df,
    [
        f.col("asst_invp_map__invp_id") == coalesced_invp_id,
        f.col("asst_invp_map__version_id") == coalesced_version_id
    ],
    "fullouter",
).withColumn(
    "misjoin_asst_invp_map_to_invp",
    f.when(
        f.col("asst_invp_map__invp_id").isNotNull() & coalesced_invp_id.isNull(),
        "Asset-Invp Map references Invp/Version not found in Invp structure"
    ).when(
        f.col("asst_invp_map__invp_id").isNull() & coalesced_invp_id.isNotNull(),
        "Invp structure exists but has no Asset mapped to it"
    ).otherwise(f.lit(None))
)


# =================================================================================
# 4. INVP TO FUND
# Naming convention: fund_dim_*
# =================================================================================

bronze_fund_dim_core_df = (
    spark.table(f"{source_db}.bronze_fund_dim_core")
    .filter("END_AT is null")
    .selectExpr("fund_core_id AS fund_dim_fund_id", "company_core_id AS fund_dim_company_id")
)

multi_join_step_three = multi_join_step_two.join(
    bronze_fund_dim_core_df,
    f.col("invp_dim__fund_id") == f.col("fund_dim_fund_id"),
    "fullouter",
).withColumn(
    "misjoin_invp_to_fund",
    f.when(f.col("invp_dim__fund_id").isNotNull() & f.col("fund_dim_fund_id").isNull(), "Invp Dim references a Fund missing from Fund Dim")
     .otherwise(f.lit(None))
)


# =================================================================================
# 5. FUND/ASSET TO RELATIONSHIP MAP (CLOSING THE CIRCLE)
# Naming convention: rel_asst_invp_map__*
# =================================================================================

company_parent_relationship_asst_invp_map__df = (
    spark.table(f"{source_db}.bronze_company_parent_relationship_map")
    .filter("END_AT is null")
    .selectExpr("ultimate_parent_id AS rels_ultimate_parent_id", "ultimate_child_id AS rels_ultimate_child_id")
    .distinct()
)

multi_join_step_four = multi_join_step_three.join(
    company_parent_relationship_asst_invp_map__df,
    [
        f.col("rels_ultimate_parent_id") == f.col("fund_dim_company_id"),
        f.col("rels_ultimate_child_id") == f.col("asset_dim__company_id"),
    ],
    "fullouter",
).withColumn(
    "misjoin_circle_to_rel_map",
    f.when(
        f.col("asset_dim__company_id").isNotNull() & f.col("fund_dim_company_id").isNotNull() & f.col("rels_ultimate_parent_id").isNull(),
        "Asset and Fund exist, but are NOT linked in Company Parent Relationship Map"
    ).when(
        f.col("asset_dim__company_id").isNull() & f.col("fund_dim_company_id").isNull() & f.col("rels_ultimate_parent_id").isNotNull(),
        "Relationship Map link exists, but missing from Fund Dim or Asset tables"
    ).otherwise(f.lit(None))
)


# =================================================================================
# 6. FINAL SELECT & WRITE
# =================================================================================

# Dynamically gather all misjoin columns
misjoin_cols = [c for c in multi_join_step_four.columns if c.startswith("misjoin")]
coalesce_expression = "COALESCE( " + ", ".join(misjoin_cols) + ") as issue"

final_columns = [
    # Asset
    "asset_dim__asset_id", "asset_dim__company_id",
    # Mapping
    "misjoin_asset_to_map", "asst_invp_map__asset_id", "asst_invp_map__invp_id", "asst_invp_map__version_id",
    # Invp
    "misjoin_invp_internal", "invp_dim__invp_id", "invp_brg__invp_id", "invp_brg__version_id", "invp_vers_version_id", "invp_dim__fund_id",
    # Map to Invp
    "misjoin_asst_invp_map_to_invp",
    # Fund
    "misjoin_invp_to_fund", "fund_dim_fund_id", "fund_dim_company_id",
    # Rel Map
    "misjoin_circle_to_rel_map", "rels_ultimate_parent_id", "rels_ultimate_child_id"
]

multi_join_step_four = (
    multi_join_step_four.select(*final_columns)
    .selectExpr("*", coalesce_expression)
    .selectExpr("*", "CASE WHEN issue IS NOT NULL THEN 1 ELSE 0 END AS total_issues")
)

# Optional: preview data before writing
check_misjoins(multi_join_step_four)

issues_df = multi_join_step_four.filter(f.col("total_issues") == 1)

if not issues_df.isEmpty():
    print(f"Writing to {issues_fqtn}")
    issues_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_fqtn)
else:
    spark.sql(f"DROP TABLE IF EXISTS {issues_fqtn}")

# COMMAND ----------

