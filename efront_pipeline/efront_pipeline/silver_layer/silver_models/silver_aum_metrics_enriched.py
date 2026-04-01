# Databricks notebook source
# DBTITLE 1,Imports
import json 
import os
import sys
from pyspark.sql import functions as F, types as T, Window as w
from pyspark.sql.types import NumericType, StringType
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Widegets & Params
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

# DBTITLE 1,Environment Setup
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

# DBTITLE 1,Load data & joins
df_aum = (
    spark.table(f"{silver_tbl_prefix}aum_metrics")
).alias("df_aum")

mfb = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_mapping_fund")
    .where(F.col("source_system_id") == F.lit("SRCE_SYST_1001"))
    .distinct()
    .alias("mfb")
)

mpb = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_mapping_portfolio")
    .where(F.col("source_system_id") == F.lit("SRCE_SYST_1001"))
    .distinct()
    .alias("mpb")
)

fund_core = spark.table("oegen_data_prod_prod.core_data_model.bronze_fund_dim_core").alias("fund_core").where(F.col("fund_core.END_AT").isNull()).distinct()
fund_names = spark.table("oegen_data_prod_prod.core_data_model.bronze_fund_dim_names").alias("fund_names").where(F.col("fund_names.END_AT").isNull()).distinct()
fund_struct = spark.table("oegen_data_prod_prod.core_data_model.bronze_fund_dim_structure_type").filter(F.col("END_AT").isNull())

invp = spark.table("oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core").alias("invp").where(F.col("invp.END_AT").isNull()).select(F.col("investment_portfolio_id"), F.col("investment_portfolio_name")).distinct()
invp_details = spark.table("oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core").filter(F.col("END_AT").isNull())

ho = (
    spark.table("oegen_data_prod_prod.core_data_model.historic_ownership")
    .alias("ho")
    .where(
        (F.col("lifecycle_milestone_type_id").isin("MILE_TYPE_1001", "MILE_TYPE_1003") == False) # Not baselines/forecasts
        & (F.col("ownership_milestone_type_id").isin("MILE_TYPE_1001", "MILE_TYPE_1003") == False)
        & ((F.col("questionable") == F.lit("false")) | (F.col("questionable") == False))
        & (F.col("ownership_phase_id").isin("OWNR_PHSE_1002", "OWNR_PHSE_1003", "OWNR_PHSE_1004") == True) # ownership phase is Acquisition/Acquired/Divesting
        & (F.col("effective_path_ownership") > 0)
    )
)

fund = (
    fund_core
    .join(
        fund_names,
        F.col("fund_core.primary_fund_name_id") == F.col("fund_names.fund_name_id"),
        "left"
    )
    .select(
        F.col("fund_core.fund_core_id").alias("fund_core_id"),
        F.col("fund_names.fund_display_name").alias("fund_display_name")
    )
).alias("fund")

mapped_efront = (
    df_aum
    .join(
        mfb,
        (F.col("df_aum.ef_fund_id") == F.col("mfb.source_fund_id")) &
        (F.col("mfb.source_system_id") == F.lit("SRCE_SYST_1001")), # eFront
        "left"
    )
    .join(
        mpb,
        (F.col("df_aum.ef_portfolio") == F.col("mpb.source_portfolio_name")) &
        (F.col("mpb.source_system_id") == F.lit("SRCE_SYST_1001")), # eFront
        "left"
    )
    .join(
        fund,
        F.col("mfb.cdm_fund_id") == F.col("fund.fund_core_id"),
        "left"
    )
    .join(
        invp,
        F.col("mpb.cdm_portfolio_id") == F.col("invp.investment_portfolio_id"),
        "left"
    )
    .select(
        F.col("mfb.cdm_fund_id"),
        F.col("mpb.cdm_portfolio_id"),
        F.col("fund.fund_display_name").alias("cdm_fund"),
        F.col("invp.investment_portfolio_name").alias("cdm_portfolio"),
        F.col("df_aum.*")
    )
).alias("mapped_efront_ids")

efront_points_in_time = (
    mapped_efront
    .select(
        F.col("cdm_fund_id").alias("f_id"),
        F.col("cdm_portfolio_id").alias("p_id"),
        F.col("REPORT_DATE").cast("date").alias("as_of_date")
    )
    .distinct()
)

cdm_points_in_time = (
    efront_points_in_time.alias("ed")
    .join(
        ho
        .select(
            "*",
            F.to_date(F.col("start")).alias("start_date"),
            F.to_date(F.col("end")).alias("end_date")
        ).alias("ho"),
        (F.col("ed.f_id") == F.col("ho.fund_core_id")) &
        (F.col("ed.p_id") == F.col("ho.investment_portfolio_id")) &
        (F.to_date(F.col("ed.as_of_date")) >= F.col("ho.start_date")) &
        (F.col("ho.end_date").isNull() | (F.to_date(F.col("ed.as_of_date")) < F.col("ho.end_date"))),
        "inner"
    )
)

cdm_assets_points_in_time = (
    cdm_points_in_time
    .select(
        F.col("ed.f_id"),
        F.col("ed.p_id"),
        F.col("ho.asset_id"),
        F.col("ed.as_of_date"),
        F.col("ho.asset"),
        F.col("ho.asset_type"),
        F.col("ho.lifecycle_phase"),
        F.col("ho.ownership_phase")
    )
)

# COMMAND ----------

# DBTITLE 1,Asset Details (Location/Tech)
asst_plt_hq = (
    spark.table("oegen_data_prod_prod.core_data_model.bronze_asset_platform_dim_core")
    .where(F.col("END_AT").isNull())
    .select(
        F.col("asset_plat_id"),
        F.col("geogprahic_hq").alias("geographic_hq")   # fix here
    )
    .distinct()
).alias("asst_plt_hq")

asst_plt_hq = asst_plt_hq.withColumnRenamed("asset_plat_id", "asset_id").withColumnRenamed("geographic_hq", "location_id")

# Get Asset Infrastructure Countries & Country Groups
asst_inf_ctry = spark.table("oegen_data_prod_prod.core_data_model.bronze_asset_infra_dim_core").alias("asst_inf_ctry")
asst_inf_ctry = asst_inf_ctry.select(F.col("asset_infra_id"), F.coalesce(F.col("country_core_id"), F.col("country_groups_id")).alias("country_id")).where(F.col("END_AT").isNull()).distinct().withColumnRenamed("asset_infra_id", "asset_id").withColumnRenamed("country_id", "location_id")

asst_loc = asst_plt_hq.unionByName(asst_inf_ctry)
asst_loc = asst_loc.alias("asst_loc")


# Get Dim Core countries & country groups from the CDM
countries = (spark.table("oegen_data_prod_prod.core_data_model.bronze_country_dim_core").alias("countries").where(F.col("END_AT").isNull()).distinct()).alias("c")
countries = countries.select(F.col("country_core_id"), F.col("country_name"))

groups = (spark.table("oegen_data_prod_prod.core_data_model.bronze_country_dim_groups").alias("groups").where(F.col("END_AT").isNull()).distinct()).alias("g")
groups = groups.select(F.col("country_groups_id"), F.col("country_group"))

# Join countries & country groups to Asset Platform & Infras
asst_loc_final = (asst_loc
    .join(countries.alias("c"), F.col("asst_loc.location_id") == F.col("c.country_core_id"), "left")
    .join(groups.alias("g"),    F.col("asst_loc.location_id") == F.col("g.country_groups_id"), "left")
    .withColumn("location", F.coalesce("country_name", "country_group"))
    .select("asst_loc.asset_id", "asst_loc.location_id", "location")
    )


asst_plt_tech = spark.table("oegen_data_prod_prod.core_data_model.bronze_asset_platform_bridge_technology").alias("asst_plt_tech")
asst_plt_tech = asst_plt_tech.select(F.col("asset_plat_id"), F.col("technology_focus")).where(F.col("END_AT").isNull()).distinct().withColumnRenamed("asset_plat_id", "asset_id").withColumnRenamed("technology_focus", "technology_name_id")

asst_infr_tech = spark.table("oegen_data_prod_prod.core_data_model.bronze_asset_infra_dim_core").alias("asst_infr_tech")
asst_infr_tech = asst_infr_tech.select(F.col("asset_infra_id"), F.col("technology_name_id")).where(F.col("END_AT").isNull()).distinct().withColumnRenamed("asset_infra_id", "asset_id")

asst_tech = asst_plt_tech.union(asst_infr_tech).alias("asst_tech")


tech_nm = spark.table("oegen_data_prod_prod.core_data_model_dev.bronze_technology_dim_names").alias("tech_nm")
tech_core_grp = spark.table("oegen_data_prod_prod.core_data_model_dev.bronze_technology_bridge_core_groups").alias("tech_core_grp")
tech_grp_bridge = spark.table("oegen_data_prod_prod.core_data_model_dev.bronze_technology_bridge_groups").alias("tech_grp_bridge")
tier1 = spark.table("oegen_data_prod_prod.core_data_model_dev.bronze_technology_dim_groups").alias("tier1")
tier2 = spark.table("oegen_data_prod_prod.core_data_model_dev.bronze_technology_dim_groups").alias("tier2")

technology_tiers = (
    tech_nm
    .join(tech_core_grp, tech_nm["technology_name_id"] == tech_core_grp["technology_name_id"], "left")
    .join(tech_grp_bridge, tech_core_grp["technology_group_id"] == tech_grp_bridge["technology_bridge_groups_id"], "left")
    .join(tier1, tech_grp_bridge["technology_grouping_tier_1"] == tier1["technology_groups_id"], "left")
    .join(tier2, tech_grp_bridge["technology_grouping_tier_2"] == tier2["technology_groups_id"], "left")
    .where(
        tech_nm["END_AT"].isNull() &
        tech_core_grp["END_AT"].isNull() &
        tech_grp_bridge["END_AT"].isNull() &
        tier1["END_AT"].isNull() &
        tier2["END_AT"].isNull()
    )
    .select(
        tech_nm["technology_name_id"],
        tech_nm["technology_name"].alias("technology_type"),
        tier1["technology_group_name"].alias("technology_sector"),
        tier2["technology_group_name"].alias("energy_system_category")
    )
).alias("tech_tiers")


# Technologies for assets are combination of TECH_CORE_xxx and TECH_NAME_xxx - we want the latter. This code makes them all TECH_NAME_xxx.
asst_tech_names_only = (
    asst_tech
    .join(
        tech_core_grp,
        asst_tech["technology_name_id"] == tech_core_grp["technology_core_id"],
        how="left"
    )
    .withColumn(
        "tech_name_id",
        F.when(F.col("asst_tech.technology_name_id").rlike(r"^TECH_NAME_"), F.col("asst_tech.technology_name_id"))  # already a name id
         .otherwise(F.col("tech_core_grp.technology_name_id"))  # mapped from core -> name via bridge
    )
    .select("asset_id", "tech_name_id")
).alias("asst_tech_names")


# Get Asset technologies as human readable words
asst_tech_final = asst_tech_names_only.join(technology_tiers.alias("tech_tiers"), F.col("asst_tech_names.tech_name_id") == F.col("tech_tiers.technology_name_id"), "left").select(F.col("asst_tech_names.asset_id"), F.col("asst_tech_names.tech_name_id"), F.col("tech_tiers.technology_type"), F.col("tech_tiers.technology_sector"), F.col("tech_tiers.energy_system_category"))


# Join CDM point in time data with asset's location and technology
cdm_assets_points_in_time_enriched = (cdm_assets_points_in_time).alias("fh").join(asst_loc_final.alias("asst_loc"), F.col("asst_loc.asset_id") == F.col("fh.asset_id"), "left").join(asst_tech_final.alias("asst_tech"), F.col("asst_tech.asset_id") == F.col("fh.asset_id"), "left").drop(F.col("asst_loc.asset_id")).drop(F.col("asst_loc.location_id")).drop(F.col("asst_tech.asset_id")).drop(F.col("asst_tech.tech_name_id")).drop(F.col("fh.asset_id"))
display(cdm_assets_points_in_time_enriched)

# COMMAND ----------

# DBTITLE 1,Aggregation & Groupings
# Get assets into their Fund + Portfolio groupings, putting asset-level details into sets which we will performing transformations/groupings on for reporting
# Also keep all the details for each asset in an array in column "asset_details"
base_sets = (
    cdm_assets_points_in_time_enriched
    .groupBy("f_id", "p_id", "as_of_date")
    .agg(
        F.collect_set("asset").alias("asset_set"),
        F.collect_set("asset_type").alias("asset_type_set"),
        F.collect_set("lifecycle_phase").alias("lifecycle_phase_set"),
        F.collect_set("ownership_phase").alias("ownership_phase_set"),
        F.collect_set("location").alias("location_set"),
        F.collect_set("technology_type").alias("technology_type_set"),
        F.collect_set("technology_sector").alias("technology_sector_set"),
        F.collect_set("energy_system_category").alias("energy_system_category_set"),

        F.collect_set(
            F.struct(
                F.col("asset").alias("name"),
                F.col("asset_type").alias("type"),
                F.col("location").alias("country"),
                F.col("technology_type").alias("technology"),
                F.col("lifecycle_phase").alias("lifecycle_phase"),
                F.col("ownership_phase").alias("ownership_phase")
            )
        ).alias("asset_details")
    )
)

df_grouped = base_sets

df_grouped = (
    df_grouped
    .withColumn("lifecycle_phase_set_orig", F.col("lifecycle_phase_set"))
    .withColumn("location_set_orig",        F.col("location_set"))
    .withColumn("technology_type_set_orig", F.col("technology_type_set"))
    .withColumn("asset_type_set_orig",      F.col("asset_type_set"))
)

# 1) lifecycle_phase_set - priority (first match wins)
phase_priority = [
    "In Last Mile Development",
    "In Development",
    "In Construction",
    "In Optimisation",
    "In Repowering",
    "Operational",
    "Decommissioning",
]

phase_expr = None
for p in phase_priority:
    cond = F.array_contains(F.col("lifecycle_phase_set"), F.lit(p))
    expr = F.array(F.lit(p))
    if phase_expr is None:
        # First condition starts the chain
        phase_expr = F.when(cond, expr)
    else:
        # Later conditions only override if *their* cond is true
        phase_expr = phase_expr.when(cond, expr)

# If none of the priority phases are present, fall back to the original set
phase_expr = phase_expr.otherwise(F.col("lifecycle_phase_set"))

df_grouped = df_grouped.withColumn("lifecycle_phase_set_grp", phase_expr)


# 2) location_set
uk_synonyms = F.array(F.lit("England"), F.lit("Scotland"), F.lit("Wales"), F.lit("UK"), F.lit("United Kingdom"))

# Map UK synonyms -> "UK"; keep others as-is
loc_mapped = F.array_distinct(
    F.transform(
        F.col("location_set"),
        lambda x: F.when(F.array_contains(uk_synonyms, x), F.lit("UK")).otherwise(x)
    )
)

# Special case: p_id = INVP_1132 and contains Portugal AND Spain -> ["Spain"]
special_case = (
    (F.col("p_id") == F.lit("INVP_1132")) &
    F.array_contains(F.col("location_set"), F.lit("Portugal")) &
    F.array_contains(F.col("location_set"), F.lit("Spain"))
)

df_grouped = df_grouped.withColumn(
    "location_set_grp",
    F.when(special_case, F.array(F.lit("Spain"))).otherwise(loc_mapped)
)


# 3) technology_type_set
# Group rules:
#  - {"Ground mount solar","Rooftop solar"} -> "Solar"
#  - {"Offshore wind","Floating wind"} -> "Offshore wind"
#  - {"Biomass","Landfill gas","Energy from waste"} -> "Biomass"

solar_syn   = F.array(F.lit("Ground mount solar"), F.lit("Rooftop solar"), F.lit("Agrivoltaics"))
offshore_syn= F.array(F.lit("Offshore wind"), F.lit("Floating wind"))
biomass_syn = F.array(F.lit("Biomass"), F.lit("Landfill gas"), F.lit("Energy from waste"))

def map_tech_element(x):
    return (
        F.when(F.array_contains(solar_syn, x),    F.lit("Solar"))
         .when(F.array_contains(offshore_syn, x), F.lit("Offshore wind"))
         .when(F.array_contains(biomass_syn, x),  F.lit("Biomass"))
         .otherwise(x)
    )

tech_mapped = F.array_distinct(
    F.transform(F.col("technology_type_set"), map_tech_element)
)

df_grouped = df_grouped.withColumn("technology_type_set_grp", tech_mapped)


# 4) asset_type_set: if contains "Platform" -> ["Platform"]
df_grouped = df_grouped.withColumn(
    "asset_type_set_grp",
    F.when(F.array_contains(F.col("asset_type_set"), F.lit("Platform")), F.array(F.lit("Platform")))
     .otherwise(F.col("asset_type_set"))
)

# (optional) tidy column order
cols = [
    "f_id","p_id","as_of_date",
    "asset_set","asset_type_set","asset_type_set_orig","asset_type_set_grp",
    "lifecycle_phase_set","lifecycle_phase_set_orig","lifecycle_phase_set_grp",
    "location_set","location_set_orig","location_set_grp",
    "technology_type_set","technology_type_set_orig","technology_type_set_grp",
    "technology_sector_set","energy_system_category_set","ownership_phase_set","asset_details"
]

_checking_groupings = df_grouped.select([c for c in cols if c in df_grouped.columns])
display(_checking_groupings)


df_final_grouping = df_grouped

# 1) lifecycle_phase_set_orig: if >1 element -> ["A & B"], else keep as-is
df_final_grouping = df_final_grouping.withColumn(
    "lifecycle_phase_set_final",
    F.when(
        F.size(F.col("lifecycle_phase_set_grp")) >= 1,
        F.array(F.element_at(F.col("lifecycle_phase_set_grp"), 1))
    ).otherwise(F.col("lifecycle_phase_set_grp"))
)


# 2) location_set_grp: if >1 country -> ["Multi-Country"], else keep as-is
df_final_grouping = df_final_grouping.withColumn(
    "location_set_final",
    F.when(F.size(F.col("location_set_grp")) > 1, F.array(F.lit("Multi-Country")))
     .otherwise(F.col("location_set_grp"))
)

# 3) technology_type_set_grp rules
# - if exactly 2 elements and one is "Battery storage" -> ["<other> & Battery storage"]
# - if >=2 elements and none is "Battery storage" -> ["Multi-Tech Renewables"]
# - otherwise keep as-is
has_battery = F.array_contains(F.col("technology_type_set_grp"), F.lit("Battery storage"))
size_tech   = F.size(F.col("technology_type_set_grp"))
other_tech  = F.element_at(
    F.array_except(F.col("technology_type_set_grp"), F.array(F.lit("Battery storage"))), 1
)

df_final_grouping = df_final_grouping.withColumn(
    "technology_type_set_final",
    F.when(
        (size_tech == 2) & has_battery,
        F.array(F.concat(other_tech, F.lit(" & Battery storage")))
    ).when(
        (size_tech >= 2),
        F.array(F.lit("Multi-Tech Renewables"))
    ).otherwise(F.col("technology_type_set_grp"))
)

# 4) Add other final columns that we haven't done a second grouping step on:
df_final_grouping = (
    df_final_grouping
    .withColumn("asset_type_set_final", F.col("asset_type_set_grp"))
    .withColumn("ownership_phase_set_final", F.col("ownership_phase_set"))
)

# Build both grouped results (comma-separated) and original/all lists (semi-colon-separated)
collapsed_df = (
    df_final_grouping
    .select(
        "f_id", "p_id", "as_of_date",

        F.col("asset_details"),

        F.concat_ws("; ", F.array_sort("asset_set")).alias("asset_names"),
        F.concat_ws("; ", F.array_sort("asset_set")).alias("reporting_asset_names"),

        # --- TYPES / ASSET TYPE ---
        F.concat_ws("; ", F.array_sort("asset_type_set_orig")).alias("asset_types"),
        F.concat_ws(", ", F.array_sort("asset_type_set_final")).alias("reporting_asset_type"),

        # --- LIFECYCLE ---
        F.concat_ws("; ", F.array_sort("lifecycle_phase_set_orig")).alias("asset_lifecycle_phases"),

        # If any asset type in the grouped set is "Platform", hard-set lifecycle to "Business / Developer". Otherwise use the normal grouped lifecycle.
        F.when(
            F.array_contains(F.col("asset_type_set_final"), F.lit("Platform"))
            , F.lit("Business / Developer")
        ).otherwise(
            F.concat_ws(", ", F.array_sort("lifecycle_phase_set_final"))
        ).alias("reporting_asset_lifecycle_phase"),
        
        # --- OWNERSHIP ---
        F.concat_ws("; ", F.array_sort("ownership_phase_set")).alias("asset_ownership_phases"),
        F.concat_ws(", ", F.array_sort("ownership_phase_set_final")).alias("reporting_asset_ownership_phase"),

        # --- LOCATION ---
        F.concat_ws("; ", F.array_sort("location_set_orig")).alias("asset_countries"),
        F.concat_ws(", ", F.array_sort("location_set_final")).alias("reporting_asset_country"),

        # --- TECHNOLOGY ---
        F.concat_ws("; ", F.array_sort("technology_type_set_orig")).alias("asset_technologies"),
        F.concat_ws(", ", F.array_sort("technology_type_set_final")).alias("reporting_asset_technology")
    )
)

df_ef_cdm_joined = (
    mapped_efront.alias("me")
    .join(
        collapsed_df.alias("c"),
        on=[
            F.col("me.cdm_fund_id") == F.col("c.f_id"),
            F.col("me.cdm_portfolio_id") == F.col("c.p_id"),
            F.col("me.REPORT_DATE") == F.col("c.as_of_date")
        ],
        how="left"
    )
).select(
    F.col("me.*"),
    F.col("c.*")
    )

df_to_save = (
    df_ef_cdm_joined
    .drop("f_id", "p_id", "as_of_date")
    .withColumn(load_date_column, F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Overrides
def override_asset_columns(df, override_cols, cond_undeployed, cond_no_asset_info):
    return reduce(
        lambda df_, c: df_.withColumn(
            c,
            F.when(cond_undeployed, F.lit("Undeployed"))
             .when(cond_no_asset_info, F.lit("N/A"))
             .otherwise(F.col(c))
        ),
        override_cols,
        df
    )

def _get_field(df_schema, colname):
    for f in df_schema.fields:
        if f.name == colname:
            return f
    return None

def _make_override_asset_details_expr(df, colname: str, value: str):
    """
    Build an expression: array(struct(...)) with all stringy fields set to `value`
    and non-string fields set to NULL (cast correctly) to respect the schema.
    """
    f = _get_field(df.schema, colname)
    if f is None:
        # Column missing: just return a literal null (safe no-op downstream)
        return F.lit(None).cast(T.ArrayType(T.StructType([])))

    dt = f.dataType
    if not isinstance(dt, T.ArrayType) or not isinstance(dt.elementType, T.StructType):
        raise TypeError(f"Column `{colname}` must be Array<Struct<...>>, got {dt.simpleString()}")

    # Build struct(<each_field := value or null>)
    struct_fields = []
    for sf in dt.elementType.fields:
        # Put the override string into string-compatible fields; null elsewhere
        if isinstance(sf.dataType, (T.StringType, T.VarcharType)) or (
            isinstance(sf.dataType, T.NullType)
        ):
            struct_fields.append(F.lit(value).cast(sf.dataType).alias(sf.name))
        else:
            struct_fields.append(F.lit(None).cast(sf.dataType).alias(sf.name))

    struct_expr = F.struct(*struct_fields)
    return F.array(struct_expr)

def override_asset_details(df, asset_details_col: str,
                           cond_undeployed, cond_no_asset_info):
    """
    When either override condition matches, replace asset_details with a
    single-item array whose struct fields are set to 'Undeployed' / 'N/A'.
    Otherwise keep the original array.
    """
    return df.withColumn(
        asset_details_col,
        F.when(
            cond_undeployed,
            _make_override_asset_details_expr(df, asset_details_col, "Undeployed")
        ).when(
            cond_no_asset_info,
            _make_override_asset_details_expr(df, asset_details_col, "N/A")
        ).otherwise(F.col(asset_details_col))
    )


# SGF fund has no asset details
cond_sgf = (F.col("ef_fund") == F.lit("Sustainable Growth Fund"))
# portfolios starting with "n/a_" have no asset details
cond_na_portfolio = F.col("ef_portfolio_id").like("n/a_%")
cond_no_asset_info = cond_sgf | cond_na_portfolio

# Undeployed portfolios
cond_undeployed = (F.col("ef_portfolio") == F.lit("Undeployed"))

OVERRIDE_COLS = [
    "asset_names",
    "reporting_asset_names",
    "asset_types",
    "reporting_asset_type",
    "asset_lifecycle_phases",
    "reporting_asset_lifecycle_phase",
    "asset_ownership_phases",
    "reporting_asset_ownership_phase",
    "asset_countries",
    "reporting_asset_country",
    "asset_technologies",
    "reporting_asset_technology",
]

df_to_save = override_asset_columns(df_to_save, OVERRIDE_COLS, cond_undeployed, cond_no_asset_info)

if "asset_details" in df_to_save.columns:
    df_to_save = override_asset_details(
        df_to_save,
        asset_details_col="asset_details",
        cond_undeployed=cond_undeployed,
        cond_no_asset_info=cond_no_asset_info
    )

display(df_to_save)

# COMMAND ----------

# DBTITLE 1,Elimination Rules
# Some elimination rules are added because otherwise the data double counts AUM due to having Fund of Funds
# Currently we have to manually add them here LOL. It's so important though as we don't want to overstate AUM
ELIMINATION_RULES = [
    {
        "cdm_fund_id": "FUND_1017",
        "portfolio_startswith": "SGF (Vector",
        "suffix": " [ELIMINATION]",
        "id_overrides": {
            "ef_fund_id": "n/a_elimination_bodge_sgf",
            "ef_portfolio_id": "n/a_elimination_bodge_sgf_vector",
            "cdm_fund_id": None,           # or "NULL"  -> will become real null
            "cdm_portfolio_id": None,
        },
    },
    {
        "cdm_fund_id": "FUND_1017",
        "portfolio_startswith": "SGF (Sky",
        "suffix": " [ELIMINATION]",
        "id_overrides": {
            "ef_fund_id": "n/a_elimination_bodge_sgf",
            "ef_portfolio_id": "n/a_elimination_bodge_sgf_sky",
            "cdm_fund_id": None,           # or "NULL"  -> will become real null
            "cdm_portfolio_id": None,
        },
    },
    {
        "cdm_fund_id": "FUND_1017",
        "portfolio_startswith": "SGF (OETF",
        "suffix": " [ELIMINATION]",
        "id_overrides": {
            "ef_fund_id": "n/a_elimination_bodge_sgf",
            "ef_portfolio_id": "n/a_elimination_bodge_sgf_oetf",
            "cdm_fund_id": None,           # or "NULL"  -> will become real null
            "cdm_portfolio_id": None,
        },
    },
]

def _get_dtype(df, colname):
    for f in df.schema.fields:
        if f.name == colname:
            return f.dataType
    return None

def apply_elimination_rules_optimized(df, rules):
    """
    Serverless-Compatible Version using Inline Explosion.
    Bypasses Catalyst Self-Union bugs by generating negated rows inline.
    """
    if not rules:
        return df, df.limit(0)

    # 1. Build a "Rule Matcher" expression to find WHICH rule applies
    rule_expr = None
    for i, rule in enumerate(rules):
        cond = (F.col("cdm_fund_id") == F.lit(rule["cdm_fund_id"]))
        if "portfolio_startswith" in rule:
            cond = cond & F.col("ef_portfolio").startswith(rule["portfolio_startswith"])
        if "portfolio_regex" in rule:
            cond = cond & F.col("ef_portfolio").rlike(rule["portfolio_regex"])

        if rule_expr is None:
            rule_expr = F.when(cond, F.lit(i))
        else:
            rule_expr = rule_expr.when(cond, F.lit(i))

    # Add the rule index (null if no rule matches)
    df_rules = df.withColumn("_rule_idx", rule_expr)

    # 2. INLINE EXPLOSION: Duplicate rows that match a rule
    # If it matches, create array [0, 1] (0 = original, 1 = negated)
    # If it doesn't match, create array [0] (original only)
    df_exploded = df_rules.withColumn(
        "_row_type",
        F.explode(
            F.when(F.col("_rule_idx").isNotNull(), F.array(F.lit(0), F.lit(1)))
             .otherwise(F.array(F.lit(0)))
        )
    )

    # 3. Apply Negation and Overrides ONLY to the new duplicated rows (_row_type == 1)
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    
    # A. Handle Negation for numerics
    for c in numeric_cols:
        df_exploded = df_exploded.withColumn(
            c,
            F.when(F.col("_row_type") == 1, -F.col(c)).otherwise(F.col(c))
        )

    # B. Handle Suffixes (Fund and Portfolio names)
    suffix_default = " [ELIMINATION]"
    for col_name in ["ef_fund", "ef_portfolio", "cdm_fund", "cdm_portfolio"]:
        if col_name not in df.columns: continue
        
        curr_expr = F.col(col_name)
        for i, rule in enumerate(rules):
            suffix = rule.get("suffix", suffix_default)
            curr_expr = F.when(
                (F.col("_row_type") == 1) & (F.col("_rule_idx") == i), 
                F.concat(F.col(col_name), F.lit(suffix))
            ).otherwise(curr_expr)
            
        df_exploded = df_exploded.withColumn(col_name, curr_expr)

    # C. Handle ID Overrides
    all_override_cols = set()
    for r in rules:
        if "id_overrides" in r:
            all_override_cols.update(r["id_overrides"].keys())

    for col_name in all_override_cols:
        if col_name not in df.columns: continue
        curr_expr = F.col(col_name)

        for i, rule in enumerate(rules):
            overrides = rule.get("id_overrides", {})
            if col_name in overrides:
                val = overrides[col_name]
                
                if val is None or (isinstance(val, str) and val.strip().upper() == "NULL"):
                    col_type = df.schema[col_name].dataType
                    lit_val = F.lit(None).cast(col_type)
                else:
                    lit_val = F.lit(val)

                curr_expr = F.when(
                    (F.col("_row_type") == 1) & (F.col("_rule_idx") == i), 
                    lit_val
                ).otherwise(curr_expr)
        
        df_exploded = df_exploded.withColumn(col_name, curr_expr)

    # 4. Split and Clean Up
    # df_final contains everything (originals + new negated rows)
    df_final = df_exploded.drop("_rule_idx", "_row_type")
    
    # added_rows isolates just the negated rows so your print statements still work perfectly
    df_added = df_exploded.filter(F.col("_row_type") == 1).drop("_rule_idx", "_row_type")

    return df_final, df_added

df_ef_with_assets, added_rows = apply_elimination_rules_optimized(df_to_save, ELIMINATION_RULES)

print("Original rows:", df_to_save.count())
print("Added rows   :", added_rows.count())
print("New total    :", df_ef_with_assets.count())
display(added_rows)

# COMMAND ----------

# DBTITLE 1,Final Enrichment & Cleanup
df_fund_structs = (
    fund_core
    .join(
        fund_struct.select("fund_structure_type_id", "fund_structure_type_name"),
        on="fund_structure_type_id",
        how="inner"
    )
)

df_fund_structs_agg = (
    df_fund_structs
    .groupBy("fund_core_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("fund_structure_type_name")))
         .alias("fund_structure_type_name")
    )
)

df_fund_structs_agg = df_fund_structs_agg.select(
    *[
        F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
        for c in df_fund_structs_agg.columns
    ]
)

invp_details_agg = (
    invp_details
    .groupBy("investment_portfolio_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("inv_pipeline_type"))).alias("inv_pipeline_type"),
        F.concat_ws(", ", F.sort_array(F.collect_set("invp_investment_strategy"))).alias("invp_investment_strategy")
    )
)

invp_details_agg = invp_details_agg.select(
    *[
        F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
        for c in invp_details_agg.columns
    ]
)

df_final = df_ef_with_assets.join(
                                invp_details_agg.select("investment_portfolio_id", "inv_pipeline_type", "invp_investment_strategy"),
                                on=[F.col("investment_portfolio_id") == F.col("cdm_portfolio_id")],
                                how="left"
                            ).join(
                                df_fund_structs_agg.select("fund_core_id", "fund_structure_type_name"),
                                on=[F.col("fund_core_id") == F.col("cdm_fund_id")],
                                how="left"
                            ).withColumn(
                                "inv_pipeline_type",
                                F.when(cond_undeployed, F.lit("Undeployed")).otherwise(F.col("inv_pipeline_type"))
                            ).withColumn(
                                "invp_investment_strategy",
                                F.when(cond_undeployed, F.lit("Undeployed")).otherwise(F.col("invp_investment_strategy"))
                            )

df_final_updated = df_final

# 1. Update Portfolio-related columns
portfolio_cols_to_update = [c for c in invp_details_agg.columns if c in df_final_updated.columns]
for c in portfolio_cols_to_update:
    df_final_updated = df_final_updated.withColumn(
        c,
        F.when(
            (F.col("ef_portfolio_id").rlike("(?i)^n/a")) & 
            (~F.col("ef_portfolio_id").rlike("(?i)^Undeployed")),
            F.lit("N/A")
        ).otherwise(F.col(c))
    )

# 2. Update Fund-related columns
fund_cols_to_update = [c for c in df_fund_structs_agg.columns if c in df_final_updated.columns]
for c in fund_cols_to_update:
    df_final_updated = df_final_updated.withColumn(
        c,
        F.when(
            (F.col("ef_fund_id").rlike("(?i)^n/a")) & 
            (~F.col("ef_fund_id").rlike("(?i)^Undeployed")),
            F.lit("N/A")
        ).otherwise(F.col(c))
    )

# 3. Drop unwanted columns
df_final_updated = df_final_updated.drop("investment_portfolio_id", "fund_core_id")

# ===================================================================
# CRITICAL BUG FIX: SPARK CATALYST LINEAGE BREAKER
# ===================================================================
# WHY THIS IS HERE:
# Earlier in the script, we heavily mutated 'ef_fund_id' and duplicated 
# rows using F.explode() for the elimination rules. Because Spark uses 
# "Lazy Evaluation," the Catalyst Optimizer tries to track this massive 
# history. When it hits the Window functions below, Catalyst gets confused, 
# looks for a "ghost" column ID from before the explosion, and crashes 
# with an [INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND] exception.
#
# WHY LOCAL CHECKPOINT:
# Normally, we would break the lineage using df.cache() or by writing a 
# temporary table. However, Databricks Serverless blocks .cache() / PERSIST, 
# and Unity Catalog restricts writing temp tables to the default schema. 
# 
# .localCheckpoint(eager=True) is the 100% Serverless-safe workaround. 
# It forces Spark to instantly execute the plan up to this point and write 
# the results to the compute node's ephemeral scratch disk. This cleanly 
# severs the corrupted Catalyst history, giving the upcoming Window functions 
# a completely fresh set of column IDs to work with.
# ===================================================================
df_final_updated = df_final_updated.localCheckpoint(eager=True)
# ===================================================================

# Keep only columns that actually exist (safe on schema drift)
OVERRIDE_COLS = [c for c in OVERRIDE_COLS if c in df_final_updated.columns]

# Force asset_details into the override list if it exists in the dataframe
if "asset_details" in df_final_updated.columns and "asset_details" not in OVERRIDE_COLS:
    OVERRIDE_COLS.append("asset_details")

# Define the forward-fill window using F.expr() to force late-binding
w_ffill = (
    w.partitionBy(F.expr("ef_fund_id"), F.expr("ef_portfolio_id"))
    .orderBy(F.expr("REPORT_DATE"))
    .rowsBetween(w.unboundedPreceding, w.currentRow)
)

# ==========================================
# FLATTEN THE WINDOW PLAN
# ==========================================
select_exprs = []
per_col_flags = []
overwritten_cols = set(OVERRIDE_COLS)

# Build expressions for the columns we are overriding
for c in OVERRIDE_COLS:
    col_type = df_final_updated.schema[c].dataType
    
    if isinstance(col_type, StringType):
        # Strings: treat "" AND NULL as missing
        c_clean = F.when((F.col(c) == "") | F.col(c).isNull(), None).otherwise(F.col(c))
    else:
        # Complex types: Only treat NULL as missing
        c_clean = F.when(F.col(c).isNull(), None).otherwise(F.col(c))

    # Forward fill 
    c_filled = F.last(c_clean, ignorenulls=True).over(w_ffill)
    
    # Flag if backfilled
    c_backfilled_flag = F.when(c_clean.isNull() & c_filled.isNotNull(), F.lit(True)).otherwise(F.lit(False))
    flag_name = f"{c}__was_backfilled_internal"
    per_col_flags.append(flag_name)

    select_exprs.append(c_filled.alias(c))
    select_exprs.append(c_backfilled_flag.alias(flag_name))

# Add all the other columns that we are NOT overriding
for c in df_final_updated.columns:
    if c not in overwritten_cols:
        select_exprs.append(F.col(c))

# Execute as a single select statement
df_ff = df_final_updated.select(*select_exprs)

# Add the single dataset-level 'backfilled' boolean
df_ff = df_ff.withColumn("backfilled", F.coalesce(F.reduce(F.array(*[F.col(f) for f in per_col_flags]), F.lit(False), lambda acc, x: acc | x), F.lit(False)))

# Drop the internal per-column flags
df_ff = df_ff.drop(*per_col_flags)

# Preserve previous behavior for NON-backfilled string columns only
string_cols = [f.name for f in df_ff.schema.fields if isinstance(f.dataType, StringType)]
non_backfill_string_cols = [c for c in string_cols if c not in OVERRIDE_COLS]

df_ff = df_ff.select(
    [
        F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
        if c in non_backfill_string_cols else F.col(c)
        for c in df_ff.columns
    ]
)

display(df_ff)

# COMMAND ----------

# DBTITLE 1,Save to delta
print("Saving data to Delta...")

if save_mode == "append":
    append_to_delta(spark, df_ff, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "overwrite":
    overwrite_to_delta(spark, df_ff, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "merge":
    merge_into_delta(spark,
                     df=df_ff,
                     target=f"{target_tbl_prefix}{tgt_table}",
                     match_keys=match_keys,
                     period_column=period_column)
else:
    raise Exception(f"Invalid save mode: {save_mode}")

# COMMAND ----------

