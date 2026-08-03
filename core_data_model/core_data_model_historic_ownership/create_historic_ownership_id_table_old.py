# Databricks notebook source
# dbutils.widgets.text("catalog", "")
# dbutils.widgets.text("database", "")
# dbutils.widgets.text("table", "")
# dbutils.widgets.text("common_path", "common")
# dbutils.widgets.text("bundle_target", "dev")
# dbutils.widgets.text("bundle_root_path", "")

import os
import sys
from pyspark.sql import functions as F, Window as W
from pyspark.sql.functions import broadcast

CATALOG = dbutils.widgets.get("catalog").strip()
DATABASE = dbutils.widgets.get("database").strip()
TABLE = dbutils.widgets.get("table").strip()
COMMON_PATH = dbutils.widgets.get("common_path").strip()
BUNDLE_TARGET = dbutils.widgets.get("bundle_target").strip()

if not CATALOG or not DATABASE or not TABLE:
    raise ValueError(f"Missing required parameters: CATALOG='{CATALOG}', DATABASE='{DATABASE}', TABLE='{TABLE}'")

FULL_SCHEMA_NAME = f"{CATALOG}.{DATABASE}"
FULL_TABLE_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"
print("Writing to:", FULL_TABLE_NAME)

# COMMAND ----------

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

        path_to_funcs = os.path.join(REPO_ROOT, COMMON_PATH)
        if COMMON_PATH not in sys.path and os.path.exists(path_to_funcs):
            sys.path.append(path_to_funcs)
        print(f"✅ Appended common libs from: {path_to_funcs}")
        
    else:
        raise FileNotFoundError("Could not find the repository root. Ensure databricks.yml exists at the root.")

else:
    REPO_ROOT = dbutils.widgets.get("bundle_root_path").strip()
    if COMMON_PATH and os.path.exists(COMMON_PATH):
        sys.path.append(COMMON_PATH)
        print(f"✅ Appended common path: {COMMON_PATH}")
    else:
        raise FileNotFoundError(f"Common path not found: {COMMON_PATH}")

from funcs.TemporalToolkit import TemporalToolkit

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepping Company Relationship Map Table

# COMMAND ----------

FAR_FUTURE_TS = F.to_timestamp(F.lit("9999-12-31 23:59:59"))

# COMMAND ----------

# Get only full relationships (where the parent is a fund and the child is an assetCo)
unique_full_rels_query = f'''
    WITH unique_full_rels AS (
    SELECT
        fdc.fund_core_id,
        rm.rel_id,
        rm.ultimate_parent_id,
        rm.ultimate_child_id,
        rm.transaction_date
    FROM {FULL_SCHEMA_NAME}.bronze_company_parent_relationship_map rm
    FULL OUTER JOIN {FULL_SCHEMA_NAME}.bronze_fund_dim_core fdc
        ON rm.ultimate_parent_id = fdc.company_core_id
    WHERE
        rm.ultimate_parent_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_fund_dim_core WHERE END_AT IS NULL)
        AND (
        rm.ultimate_child_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_asset_infra_dim_core WHERE END_AT IS NULL)
        OR
        rm.ultimate_child_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core WHERE END_AT IS NULL)
        )
        AND rm.END_AT IS NULL
        AND fdc.END_AT IS NULL
    )
    SELECT
    fund_core_id, rel_id, ultimate_parent_id, ultimate_child_id,
    MIN(transaction_date) AS earliest_transaction_date
    FROM unique_full_rels
    GROUP BY fund_core_id, rel_id, ultimate_parent_id, ultimate_child_id;
 '''
unique_full_rels_raw = sql(unique_full_rels_query)
# display(unique_full_rels_raw)

# COMMAND ----------

df = unique_full_rels_raw.withColumn(
    "raw_components",
    F.filter(
        F.transform(
            F.split("rel_id", "(?=COMP_)"),
            lambda x: F.regexp_extract(x, r"(COMP_[A-Za-z0-9]+)", 1)
        ),
        lambda x: x != ""
    )
)

# remove only consecutive duplicates (A,A,B,B,C -> A,B,C)
df = df.withColumn(
    "company_chain_id",
    F.expr("""
      aggregate(
        raw_components,
        CAST(array() AS array<string>),
        (acc, x) -> IF(size(acc)=0 OR element_at(acc, size(acc)) <> x,
                       concat(acc, array(x)),
                       acc)
      )
    """)
)

df = df.withColumn(
    "parent_child_pairs",
    F.when(
        F.size("company_chain_id") >= 2,
        F.expr("""
          transform(
            arrays_zip(
              slice(company_chain_id, 1, size(company_chain_id)-1),
              slice(company_chain_id, 2, size(company_chain_id)-1)
            ),
            x -> array(x['0'], x['1'])
          )
        """)
    ).otherwise(F.array())
)

# display(df)

# COMMAND ----------

distinct_pairs_df = (
    df
    .withColumn("pair", F.explode("parent_child_pairs"))  # flatten to one pair per row
    .select("pair")
    .distinct()
)

distinct_pairs_list_df = distinct_pairs_df.agg(F.collect_set("pair").alias("distinct_parent_child_pairs"))
# display(distinct_pairs_list_df)

# COMMAND ----------

# Turn parent-child pairs into rel_id pattern -> "COMP_A.COMP_B"
pairs_rel_ids_df = (
    distinct_pairs_df
    .select(F.concat_ws(".", F.col("pair")).alias("rel_id"))
    .distinct()
)
# display(pairs_rel_ids_df)

# COMMAND ----------

# Read the relationship map
rel_map_active_query = f"""
    SELECT 
        rel_id,
        percentage,
        transaction_date,
        ultimate_child_id,
        ultimate_parent_id,
        relationship_level
    FROM {FULL_SCHEMA_NAME}.bronze_company_parent_relationship_map
    WHERE END_AT IS NULL
"""
rel_map_active_df = spark.sql(rel_map_active_query)

# COMMAND ----------

# Semi-join to keep only rows whose rel_id is in our pairs list
#    (use broadcast as pairs list is small for efficiency)
result_df = (
    rel_map_active_df
    .join(F.broadcast(pairs_rel_ids_df), on="rel_id", how="semi")
    .select(
        "rel_id",
        "percentage",
        "transaction_date",
        "ultimate_child_id",
        "ultimate_parent_id",
        "relationship_level",
    )
)
# display(result_df)

# COMMAND ----------

# Create active_from and active_to date from transaction_date for each rel_id 
w = W.partitionBy("rel_id").orderBy(F.col("transaction_date"), F.col("relationship_level"))

result_with_intervals = (
    result_df
    # ensure deterministic ordering and no exact dup rows
    .dropDuplicates(["rel_id", "transaction_date", "ultimate_child_id", "ultimate_parent_id", "relationship_level", "percentage"])
    .withColumn("active_from_date", F.col("transaction_date"))
    .withColumn("active_to_date", F.lead("transaction_date").over(w))
)
# display(result_with_intervals)

# COMMAND ----------

paths_df = df.select(
    "fund_core_id",
    F.col("rel_id").alias("full_rel_id"),
    "ultimate_child_id",
    "ultimate_parent_id",
    "earliest_transaction_date",
    "company_chain_id",
    "parent_child_pairs"
)

path_pairs = (
    paths_df
    .select(
        "fund_core_id", "full_rel_id", "company_chain_id",
        "ultimate_child_id", "ultimate_parent_id", "earliest_transaction_date",
        F.posexplode("parent_child_pairs").alias("pair_pos", "pair")
    )
    .withColumn("pair_rel_id", F.concat_ws(".", F.col("pair")))
)

w_pp = W.partitionBy("fund_core_id","full_rel_id","company_chain_id","pair_pos") \
        .orderBy(F.col("pair_rel_id"))

path_pairs_deduped = (
  path_pairs
    .withColumn("_rn", F.row_number().over(w_pp))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)
# display(path_pairs_deduped)

# COMMAND ----------

# result_with_intervals has each parent-child edge with windows & percentage
# Here we prepare pair intervals (cap open-ended active_to with a far-future date so we can intersect ranges cleanly)

# pair_rel_id   | percentage | active_from | active_to
# ------------- | ---------- | ----------- | --------------
# COMP_A.COMP_B | 60         | 2019-01-01  | 2020-01-01
# COMP_A.COMP_B | 55         | 2020-01-01  | null            (open-ended) <-- change this to "9999-12-31 23:59:59"
# COMP_B.COMP_C | 80         | 2018-06-01  | 2019-06-01
# COMP_B.COMP_C | 70         | 2019-06-01  | 2020-03-01
# COMP_B.COMP_C | 65         | 2020-03-01  | null            (open-ended) <-- change this to "9999-12-31 23:59:59"

pair_intervals = (
    result_with_intervals
    .withColumnRenamed("rel_id", "pair_rel_id")
    .withColumn("active_to_date", F.coalesce(F.col("active_to_date"), FAR_FUTURE_TS))
    .select("pair_rel_id", "percentage", "active_from_date", "active_to_date")
)
# display(pair_intervals)

# COMMAND ----------

# Attach intervals to every pair in each rel_id path. Now each path carries all intervals for each pair it uses.
# Why we need this: a path spans multiple pairs, and each pair can change at different dates. To compute path-wise valid intervals, we need to consider all change points across all pairs in the path.

# ==================================================================================================================================

# path_pairs_deduped:

# fund_core_id | full_rel_id                | pair             | pair_rel_id
# ------------ | -------------------------- | ---------------- | --------------
# FUND_1       | COMP_A.COMP_B_COMP_B.COMP_C| ["A","B"]        | COMP_A.COMP_B
# FUND_1       | COMP_A.COMP_B_COMP_B.COMP_C| ["B","C"]        | COMP_B.COMP_C

# ==================================================================================================================================

# pair_intervals:

# pair_rel_id   | percentage | active_from | active_to
# ------------- | ---------- | ----------- | -------------------
# COMP_A.COMP_B | 60         | 2019-01-01  | 2020-01-01
# COMP_A.COMP_B | 55         | 2020-01-01  | 9999-12-31 23:59:59
# COMP_B.COMP_C | 80         | 2018-06-01  | 2019-06-01
# COMP_B.COMP_C | 70         | 2019-06-01  | 2020-03-01
# COMP_B.COMP_C | 65         | 2020-03-01  | 9999-12-31 23:59:59

# ==================================================================================================================================

# path_pair_intervals = path_pairs_deduped joined to pair_intervals

# fund_core_id | full_rel_id                | pair_rel_id   | percentage | active_from | active_to
# ------------ | -------------------------- | ------------- | ---------- | ----------- | -------------------
# FUND_1       | COMP_A...B...C             | COMP_A.COMP_B | 60         | 2019-01-01  | 2020-01-01
# FUND_1       | COMP_A...B...C             | COMP_A.COMP_B | 55         | 2020-01-01  | 9999-12-31
# FUND_1       | COMP_A...B...C             | COMP_B.COMP_C | 80         | 2018-06-01  | 2019-06-01
# FUND_1       | COMP_A...B...C             | COMP_B.COMP_C | 70         | 2019-06-01  | 2020-03-01
# FUND_1       | COMP_A...B...C             | COMP_B.COMP_C | 65         | 2020-03-01  | 9999-12-31

# ==================================================================================================================================


path_pair_intervals = (
    path_pairs_deduped
    .join(F.broadcast(pair_intervals), on="pair_rel_id", how="inner")
    .select(
        "fund_core_id", "full_rel_id", "company_chain_id", "earliest_transaction_date", "pair", "pair_rel_id",
        "ultimate_child_id", "ultimate_parent_id", "percentage", "active_from_date", "active_to_date"
    )
    # Trim: no activity before the path's earliest_transaction_date
    .withColumn(
        "active_from_date",
        F.greatest(F.col("active_from_date"), F.col("earliest_transaction_date"))
    )
    # If the (trimmed) interval has inverted/empty bounds, drop it
    .where(F.col("active_to_date") > F.col("active_from_date"))
)
# display(path_pair_intervals)

# COMMAND ----------

# Build *global* change boundaries per path (union of all pair starts/ends)
# Collect all starts and ends from all pair intervals in a path, dedupe, sort them, and then turn them into minimal segments between consecutive boundaries
# Within any segment, no pair changes occur (changes only happen at boundaries)

# e.g. boundaries could be [2018-06-01, 2019-01-01, 2019-06-01, 2020-01-01, 2020-03-01, 9999-12-31]
#      so segments are then the consecutive pairs:

#           [2018-06-01,2019-01-01],
#           [2019-01-01,2019-06-01],
#           [2019-06-01,2020-01-01],
#           [2020-01-01,2020-03-01],
#           [2020-03-01,9999-12-31]


path_boundaries = (
    path_pair_intervals
    .groupBy("fund_core_id", "full_rel_id", "company_chain_id",)
    .agg(
        F.array_sort(
            F.array_distinct(
                F.array_union(
                    F.flatten(F.collect_list(F.array("active_from_date", "active_to_date"))),
                    F.collect_set("earliest_transaction_date")
                )
            )
        ).alias("boundaries")
    )
    .withColumn(
        "segments",
        F.expr("""
          transform(
            sequence(1, size(boundaries)-1),
            i -> struct(boundaries[i-1] as seg_start, boundaries[i] as seg_end)
          )
        """)
    )
    .select("fund_core_id", "full_rel_id", "company_chain_id", F.explode("segments").alias("seg"))
    .select(
        "fund_core_id",
        "full_rel_id",
        "company_chain_id",
        F.col("seg.seg_start").alias("seg_start"),
        F.col("seg.seg_end").alias("seg_end")
    )
)
# display(path_boundaries)

# Keep only segments that are covered by ALL pairs in the path
# For each path+segment, check coverage by joining segments with pair intervals and counting how many distinct pairs cover the entire segment:
#       Segment is covered by a pair if --> seg_start >= active_from AND seg_end <= active_to
#       We can find the number of pairs in the path (2 in our example: COMP_A.COMP_B and COMP_B.COMP_C or A-->B and B-->C).
#       Keep only segments where covered_pairs == num_pairs.
#       Example coverage:
#           [2018-06-01, 2019-01-01]: only B-->C is active --> drop (needs both A-->B and B-->C).
#           All later segments are covered by both pairs --> keep.

# a) Count how many distinct pairs are in each path
pairs_per_path = (
    path_pairs_deduped
    .groupBy("fund_core_id", "full_rel_id", "company_chain_id")
    .agg(F.countDistinct("pair_rel_id").alias("num_pairs"))
)

# b) Count coverage per (path, segment) by joining segments to intervals
segment_coverage = (
    path_boundaries
    .join(path_pair_intervals, on=["fund_core_id", "full_rel_id", "company_chain_id"], how="inner")
    .where((F.col("seg_start") >= F.col("active_from_date")) & (F.col("seg_end") <= F.col("active_to_date")))
    .groupBy("fund_core_id", "full_rel_id", "company_chain_id", "seg_start", "seg_end")
    .agg(F.countDistinct("pair_rel_id").alias("covered_pairs"))
    .join(pairs_per_path, on=["fund_core_id", "full_rel_id", "company_chain_id"], how="inner")
    .where(F.col("covered_pairs") == F.col("num_pairs"))  # segments valid for ALL pairs
    .select("fund_core_id", "full_rel_id", "company_chain_id", "seg_start", "seg_end")
)

# COMMAND ----------

# Emit final rows per (full_rel_id, parent_child_pair, active_from, active_to, percentage)
# Join valid segments back to the per-pair intervals/windows and filter to keep those pairs whose window covers the segment.
# For each kept (path, segment), we get one row per pair with that pair's percentage ON that segment.

# Our example:
# fund_core_id | full_rel_id     | parent_child_pair | pair_rel_id    | percentage | active_from  | active_to
# ------------ | --------------- | ----------------- | -------------- | ---------- | ------------ | -------------
# FUND_1       | COMP_A...B...C  | ["COMP_A","COMP_B"]| COMP_A.COMP_B  | 60         | 2019-01-01   | 2019-06-01
# FUND_1       | COMP_A...B...C  | ["COMP_B","COMP_C"]| COMP_B.COMP_C  | 80         | 2019-01-01   | 2019-06-01

# FUND_1       | COMP_A...B...C  | ["COMP_A","COMP_B"]| COMP_A.COMP_B  | 60         | 2019-06-01   | 2020-01-01
# FUND_1       | COMP_A...B...C  | ["COMP_B","COMP_C"]| COMP_B.COMP_C  | 70         | 2019-06-01   | 2020-01-01

# FUND_1       | COMP_A...B...C  | ["COMP_A","COMP_B"]| COMP_A.COMP_B  | 55         | 2020-01-01   | 2020-03-01
# FUND_1       | COMP_A...B...C  | ["COMP_B","COMP_C"]| COMP_B.COMP_C  | 70         | 2020-01-01   | 2020-03-01

# FUND_1       | COMP_A...B...C  | ["COMP_A","COMP_B"]| COMP_A.COMP_B  | 55         | 2020-03-01   | 9999-12-31
# FUND_1       | COMP_A...B...C  | ["COMP_B","COMP_C"]| COMP_B.COMP_C  | 65         | 2020-03-01   | 9999-12-31


final_df = (
    segment_coverage.alias("sc")
    .join(
        path_pair_intervals.alias("ppi"),
        on=["fund_core_id", "full_rel_id", "company_chain_id"],
        how="inner"
    )
    .where(
        (F.col("sc.seg_start") >= F.col("ppi.active_from_date")) &
        (F.col("sc.seg_end")   <= F.col("ppi.active_to_date"))
    )
    .select(
        F.col("sc.fund_core_id").alias("fund_core_id"),
        F.col("sc.full_rel_id").alias("full_rel_id"),
        F.col("sc.company_chain_id").alias("company_chain_id"),
        F.col("ppi.earliest_transaction_date").alias("earliest_transaction_date"),
        F.col("ppi.pair").alias("parent_child_pair"),
        F.col("ppi.pair_rel_id").alias("pair_rel_id"),
        F.col("ppi.ultimate_child_id").alias("ultimate_child_id"),
        F.col("ppi.ultimate_parent_id").alias("ultimate_parent_id"),
        F.col("ppi.percentage").alias("percentage"),
        F.col("sc.seg_start").alias("active_from_date"),
        F.col("sc.seg_end").alias("active_to_date"),
    )
)

# display(final_df)

# COMMAND ----------

# Next we want to collapse the per-parent-child-pair rows ON each valid segment into a single row per (full_rel_id, active_from, active_to) with an array of pairs + percentage. To do this, we need to use the positioning of each pair in the path that we saved in the path_pairs_deduped df.

# Attach path_pairs_deduped (for deterministic ordering inside the array)
final_with_pos = (
    final_df.alias("f")
    .join(
        path_pairs_deduped
        .select("fund_core_id","full_rel_id","company_chain_id","pair_rel_id","pair_pos","pair")
        .alias("p"),
        on=["fund_core_id","full_rel_id","company_chain_id","pair_rel_id"],
        how="left"
    )
)

# display(final_with_pos)

# COMMAND ----------

# DBTITLE 1,path_pairs should be unique at edge grain
path_pairs_duplicate_check = (
  path_pairs_deduped
    .groupBy("fund_core_id","full_rel_id","company_chain_id","pair_rel_id","pair_pos")
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)
# display(path_pairs_duplicate_check)

assert path_pairs_duplicate_check.isEmpty()

# COMMAND ----------

# DBTITLE 1,final_with_pos should NOT create extra rows
final_with_pos_duplicate_check = (
  final_with_pos
    .groupBy("fund_core_id","full_rel_id","company_chain_id","pair_rel_id","active_from_date","active_to_date")
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)
# display(final_with_pos_duplicate_check)

assert final_with_pos_duplicate_check.count() == 0

# COMMAND ----------

# Group to one row per (full_rel_id, active_from, active_to), collecting pairs+percentages
grouped_segments = (
    final_with_pos
    .groupBy("fund_core_id", "full_rel_id", "company_chain_id", "ultimate_child_id", "ultimate_parent_id", "active_from_date", "active_to_date")
    .agg(
        # collect as structs so rel_id and percentage stay aligned; include order + original pair array
        F.array_sort(
            F.collect_list(
                F.struct(
                    F.col("pair_pos"),
                    F.col("pair_rel_id").alias("pair_rel_id"),
                    F.col("percentage").alias("percentage"),
                    F.col("pair").alias("parent_child_pair")  # optional: keeps ["COMP_A","COMP_B"]
                )
            )
        ).alias("pairs_with_ownership_sorted")
    )
    # drop the sorting key from the output if you don't need it
    .select(
        "fund_core_id", "full_rel_id", "company_chain_id", "ultimate_child_id", "ultimate_parent_id", "active_from_date", "active_to_date",
        F.expr("""
          transform(pairs_with_ownership_sorted, x -> struct(
            x.pair_rel_id as pair_rel_id,
            x.percentage   as ownership,
            x.parent_child_pair as parent_child_pair
          ))
        """).alias("pairs_with_ownership")
    )
)
# display(grouped_segments)

# COMMAND ----------

# Multiply edge percentages (0–100 inputs) to a single path percentage per (full_rel_id, segment)

grouped_segments_with_path_pct = (
    grouped_segments
    # Count pairs for basic integrity checks
    .withColumn("num_pairs_in_path", F.size(F.col("pairs_with_ownership")))
    .withColumn("pairs_non_null", F.expr("filter(pairs_with_ownership, x -> x.ownership is not null)"))
    .withColumn("num_pairs_with_pct", F.size(F.col("pairs_non_null")))
    # Compute product of percentages as a fraction:
    # product( (pct_i / 100.0) for i in edges )
    .withColumn(
        "path_fraction",
        F.when(
            F.col("num_pairs_with_pct") == F.col("num_pairs_in_path"),
            F.expr("""
              aggregate(
                transform(pairs_non_null, x -> cast(x.ownership as double)),
                1.0D,
                (acc, y) -> acc * y
              )
            """)
        )
        .otherwise(F.lit(None).cast("double"))  # if any ownership is null, surface as NULL to flag data issue
    )
    .withColumn("path_fraction_rounded", F.round(F.col("path_fraction"), 6))
)
# display(grouped_segments_with_path_pct)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining All Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting source datasets

# COMMAND ----------

# Full company relationships as a history table
full_rels_hist_derived = grouped_segments_with_path_pct.select(
    "fund_core_id", "full_rel_id", "company_chain_id", "ultimate_child_id", "ultimate_parent_id", "active_from_date", "active_to_date", "pairs_with_ownership", "path_fraction_rounded"
)

# COMMAND ----------

# Inv. Portfolio Dimension
invp_dim_query = f"""
SELECT
    src.investment_portfolio_id,
    src.invp_fund_core_id AS fund_core_id
FROM {FULL_SCHEMA_NAME}.bronze_investment_portfolio_dim_core_af src
LEFT JOIN {FULL_SCHEMA_NAME}.bronze_fund_dim_core bfdc ON bfdc.fund_core_id = src.invp_fund_core_id
WHERE src.END_AT IS NULL AND bfdc.END_AT IS NULL
"""
invp_dim_raw = spark.sql(invp_dim_query)

# Inv. Portfolio Bridge
invp_bridge_query = f"""
SELECT investment_portfolio_id, invp_version_id
FROM {FULL_SCHEMA_NAME}.bronze_investment_portfolio_bridge
WHERE END_AT IS NULL
"""
invp_bridge_raw = spark.sql(invp_bridge_query)

# Inv. Portfolio Version
invp_ver_query = f"""
SELECT invp_version_id, active_from_date, active_to_date
FROM {FULL_SCHEMA_NAME}.bronze_investment_portfolio_fact_version
WHERE END_AT IS NULL
"""
invp_ver_raw = spark.sql(invp_ver_query)

# Asset-AssetCos
assetco_to_asset_query = f'''
SELECT DISTINCT company_core_id AS assetco_core_id, asset_infra_id AS asset_id
FROM {FULL_SCHEMA_NAME}.bronze_asset_infra_dim_core WHERE END_AT IS NULL
UNION
SELECT DISTINCT company_core_id AS assetco_core_id, asset_plat_id AS asset_id
FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core WHERE END_AT IS NULL
'''
assetco_to_asset = spark.sql(assetco_to_asset_query).dropDuplicates(["assetco_core_id","asset_id"])

# Assets-Portfolio Mapping
asst_invp_query = f'''
    SELECT DISTINCT
        asset_id,
        investment_portfolio_id,
        active_from_date,
        active_to_date
    FROM {FULL_SCHEMA_NAME}.bronze_asset_invp
'''
asst_invp_raw = spark.sql(asst_invp_query)

# History of Platform Assets
plat_ownership_query = f'''
WITH unique_full_rels_for_platform_assets AS (
    SELECT
        rm.rel_id,
        rm.ultimate_parent_id,
        rm.ultimate_child_id,
        MIN(rm.transaction_date) AS earliest_transcation_date
    FROM {FULL_SCHEMA_NAME}.bronze_company_parent_relationship_map rm
        WHERE
            rm.ultimate_parent_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_fund_dim_core WHERE END_AT IS NULL)
            AND 
            rm.ultimate_child_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core WHERE END_AT IS NULL)
            AND rm.END_AT IS NULL
    GROUP BY rm.rel_id,
            rm.ultimate_parent_id,
            rm.ultimate_child_id
    )
    SELECT
        asset_plat_id AS asset_id,
        array(struct(
            'OWNR_MILE_1003' as milestone_id, 
            'MILE_TYPE_1002' as milestone_type_id,
            earliest_transcation_date as milestone_date
        )) AS ownership_milestone_details_id,
        'MILE_TYPE_1002' AS ownership_milestone_type_id,
        'OWNR_PHSE_1003' AS ownership_phase_id,
        earliest_transcation_date AS active_from_date,
        NULL AS active_to_date
    FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core pdc
    LEFT JOIN unique_full_rels_for_platform_assets r
        ON pdc.company_core_id=r.ultimate_child_id
    WHERE pdc.END_AT IS NULL
'''
plat_ownership_derived = spark.sql(plat_ownership_query)

plat_lifecycle_query = f'''
WITH unique_full_rels_for_platform_assets AS (
    SELECT
        rm.rel_id,
        rm.ultimate_parent_id,
        rm.ultimate_child_id,
        MIN(rm.transaction_date) AS earliest_transcation_date
    FROM {FULL_SCHEMA_NAME}.bronze_company_parent_relationship_map rm
        WHERE
            rm.ultimate_parent_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_fund_dim_core WHERE END_AT IS NULL)
            AND 
            rm.ultimate_child_id IN (SELECT company_core_id FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core WHERE END_AT IS NULL)
            --AND rm.END_AT IS NULL
    GROUP BY rm.rel_id,
            rm.ultimate_parent_id,
            rm.ultimate_child_id
    )
    SELECT
        asset_plat_id AS asset_id,
        array(struct(
            'First Transaction' as milestone_id, 
            'MILE_TYPE_1002' as milestone_type_id,
            earliest_transcation_date as milestone_date
        )) AS lifecycle_milestone_details_id,
        'MILE_TYPE_1002' AS lifecycle_milestone_type_id,
        'LIFE_PHSE_1001' AS lifecycle_phase_id,
        earliest_transcation_date AS active_from_date,
        NULL AS active_to_date
    FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core pdc
    LEFT JOIN unique_full_rels_for_platform_assets r
        ON pdc.company_core_id=r.ultimate_child_id
    WHERE pdc.END_AT IS NULL
'''
plat_lifecycle_derived = spark.sql(plat_lifecycle_query)

infra_lifecycle_raw_query = f"""
SELECT * FROM {FULL_SCHEMA_NAME}.silver_asset_lifecycle_fact WHERE END_AT IS NULL
"""
infra_lifecycle_raw = spark.sql(infra_lifecycle_raw_query)

infra_ownership_raw_query = f"""
SELECT * FROM {FULL_SCHEMA_NAME}.silver_asset_ownership_fact WHERE END_AT IS NULL
"""
infra_ownership_raw = spark.sql(infra_ownership_raw_query)


milestone_types_raw_query = f"""
SELECT milestone_type_id, viewing_order 
FROM {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core 
WHERE END_AT IS NULL
"""
priority_df = spark.sql(milestone_types_raw_query)

# =================================================================================
# Fetch Asset Metadata (Countries, Country Groups, Technologies)
# =================================================================================
tech_dim_query = f"""
    SELECT
        a.technology_name_id,
        a.technology_name,
        b.technology_core_id,
        d.technology_category_name,
        c.technology_grouping_tier_1 AS technology_grouping_tier_1_id,
        e.technology_group_name AS technology_grouping_tier_1,
        c.technology_grouping_tier_2 AS technology_grouping_tier_2_id,
        f.technology_group_name AS technology_grouping_tier_2
    FROM oegen_data_prod_prod.core_data_model.bronze_technology_dim_names a
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_technology_bridge_core_groups b
            ON a.technology_name_id = b.technology_name_id AND b.END_AT IS NULL
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_technology_bridge_groups c
            ON c.technology_bridge_groups_id = b.technology_group_id AND c.END_AT IS NULL
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_technology_dim_core d
            ON d.technology_core_id = b.technology_core_id AND d.END_AT IS NULL
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_technology_dim_groups e
            ON e.technology_groups_id = c.technology_grouping_tier_1 AND e.END_AT IS NULL
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_technology_dim_groups f
            ON f.technology_groups_id = c.technology_grouping_tier_2 AND f.END_AT IS NULL
    WHERE a.END_AT IS NULL
"""
tech_dim_df = spark.sql(tech_dim_query)

# 1. Infrastructure Metadata (Join to Country Groups bridge for inherited groups)
infra_meta_query = f"""
    SELECT 
        i.asset_infra_id AS asset_id,
        collect_set(i.country_core_id) AS country_core_id,
        collect_set(g.country_group_id) AS country_groups_id,
        collect_set(i.technology_name_id) AS technology_name_id
    FROM {FULL_SCHEMA_NAME}.bronze_asset_infra_dim_core i
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_country_bridge_core_groups g
        ON i.country_core_id = g.country_id
        AND g.END_AT IS NULL
    WHERE i.END_AT IS NULL
    GROUP BY i.asset_infra_id
"""
infra_meta_df = spark.sql(infra_meta_query)

# 2. Platform Technology Metadata
plat_tech_query = f"""
    SELECT 
        asset_plat_id AS asset_id,
        collect_set(technology_focus) AS technology_name_id
    FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_bridge_technology
    WHERE END_AT IS NULL
    GROUP BY asset_plat_id
"""
plat_tech_df = spark.sql(plat_tech_query)

# 3. Platform Geo Metadata
plat_geo_query = f"""
    SELECT 
        c.asset_plat_id AS asset_id,
        collect_set(c.geogprahic_hq) AS country_core_id,
        collect_set(g.country_group_id) AS country_groups_id
    FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core c
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_country_bridge_core_groups g
    ON c.geogprahic_hq = g.country_id
        AND g.END_AT IS NULL
    WHERE c.END_AT IS NULL
    GROUP BY c.asset_plat_id
"""
plat_geo_df = spark.sql(plat_geo_query)

empty_str_array = F.array().cast("array<string>")

# 4. Combine Platform Data and ensure arrays are initialized
plat_meta_df = (
    plat_geo_df.join(plat_tech_df, on="asset_id", how="outer")
    .withColumn("country_core_id", F.coalesce(F.col("country_core_id"), empty_str_array))
    .withColumn("country_groups_id", F.coalesce(F.col("country_groups_id"), empty_str_array))
    .withColumn("technology_name_id", F.coalesce(F.col("technology_name_id"), empty_str_array))
)

# 5. Ensure Infra arrays are initialized, then UNION
infra_meta_df = (
    infra_meta_df
    .withColumn("country_core_id", F.coalesce(F.col("country_core_id"), empty_str_array))
    .withColumn("country_groups_id", F.coalesce(F.col("country_groups_id"), empty_str_array))
    .withColumn("technology_name_id", F.coalesce(F.col("technology_name_id"), empty_str_array))
)

asset_metadata_df = infra_meta_df.unionByName(plat_meta_df)

# 6. Enrich Metadata with Hierarchical Tech IDs

# Explode the technology_name_id array to join with tech_dim_df
enriched_tech = (
    asset_metadata_df.select(
        "asset_id", 
        F.explode_outer("technology_name_id").alias("tech_name_id_exploded")
    )
    .join(
        tech_dim_df, 
        F.col("tech_name_id_exploded") == F.col("technology_name_id"), 
        how="left"
    )
    .groupBy("asset_id")
    .agg(
        # Grouping back into arrays, ignoring nulls
        F.collect_set("technology_core_id").alias("technology_core_id"), # Maps to category
        F.collect_set("technology_grouping_tier_1_id").alias("technology_grouping_tier_1_id"),
        F.collect_set("technology_grouping_tier_2_id").alias("technology_grouping_tier_2_id")
    )
)

# Join the new arrays back to the main metadata dataframe
asset_metadata_df = (
    asset_metadata_df
    .join(enriched_tech, on="asset_id", how="left")
    # Ensure empty arrays instead of nulls if no matches were found
    .withColumn("technology_core_id", F.coalesce(F.col("technology_core_id"), empty_str_array))
    .withColumn("technology_grouping_tier_1_id", F.coalesce(F.col("technology_grouping_tier_1_id"), empty_str_array))
    .withColumn("technology_grouping_tier_2_id", F.coalesce(F.col("technology_grouping_tier_2_id"), empty_str_array))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepping

# COMMAND ----------

def apply_priority_and_intervals(raw_df, priority_df, id_col_prefix):
    '''
    The Priority Filter: First, join to priority_df to get a viewing_order. For any given asset_id and milestone_id, if there are multiple records (e.g., an Actual, a Forecast, and a Baseline), it uses a Window function to pick the one with the lowest viewing_order. So, at the individual milestone level, priority wins.

    The Phase Grouping: Next, group the data to create the intervals. We group by asset_id, lifecycle_phase_id, a boolean is_future flag, and a conditional _grp_date.

    There can be multiple dates in the array for historical milestones, but NOT for future milestones.
    '''
    type_id_col = f"{id_col_prefix}_milestone_type_id"
    milestone_id_col = f"{id_col_prefix}_milestone_id"
    phase_id_col = f"{id_col_prefix}_phase_id"
    
    # Use current_date to avoid millisecond boundary mismatches
    TODAY = F.to_timestamp(F.current_date()) 

    # 1. Standard Priority Filter
    df = raw_df.join(F.broadcast(priority_df), F.col(type_id_col) == F.col("milestone_type_id"), how="inner")
    w_ms = W.partitionBy("asset_id", milestone_id_col)
    df = df.withColumn("_best_type", F.min("viewing_order").over(w_ms)) \
           .filter(F.col("viewing_order") == F.col("_best_type"))

    # 2. Identify the last "Actual" (MILE_TYPE_1002) for each asset
    w_actual = W.partitionBy("asset_id").orderBy(F.col("event_date").desc())
    df = df.withColumn(
        "_last_actual_date", 
        F.max(F.when(F.col(type_id_col) == "MILE_TYPE_1002", F.col("event_date"))).over(W.partitionBy("asset_id"))
    )

    # 3. Collapse to Phase Intervals
    df = df.withColumn("is_future", F.col("event_date") > TODAY)
    df = df.withColumn("_grp_date", F.when(F.col("is_future"), F.col("event_date")).otherwise(F.lit(None)))
    
    phase_intervals = (
        df.groupBy("asset_id", phase_id_col, "is_future", "_grp_date")
        .agg(
            F.min("event_date").alias("phase_start_date"),
            # Return the type_id_col associated with the minimum viewing_order in this group
            F.expr(f"min_by({type_id_col}, viewing_order)").alias(type_id_col),

            # Pack all milestone details (including type) into the array
            F.collect_list(
                F.struct(
                    F.col(milestone_id_col).alias("milestone_id"),
                    F.col(type_id_col).alias("milestone_type_id"),
                    F.col("event_date").alias("milestone_date")
                )
            ).alias(f"{id_col_prefix}_milestone_details_id")
        )
    )

    # 4. Create Timeline
    w_ev = W.partitionBy("asset_id").orderBy("phase_start_date")
    timeline = (
        phase_intervals
        .withColumn("active_from_date", F.col("phase_start_date").cast("timestamp"))
        .withColumn("active_to_date", F.lead("phase_start_date").over(w_ev).cast("timestamp"))
    )

    # 5. The "Actuals Extension" Logic: 
    # If the current phase is Forecast/Baseline AND it starts <= TODAY AND there is a preceding Actual
    # we inject a logic check here that we will use later in the Questonable flag.
    return timeline.drop("phase_start_date", "is_future", "_grp_date")

infra_lifecycle_intervals = apply_priority_and_intervals(infra_lifecycle_raw, priority_df, "lifecycle")
infra_lifecycle_intervals = infra_lifecycle_intervals.select(
        "asset_id",
        "lifecycle_phase_id",
        "lifecycle_milestone_type_id",
        "lifecycle_milestone_details_id",
        "active_from_date",
        "active_to_date"
    )
infra_ownership_intervals = apply_priority_and_intervals(infra_ownership_raw, priority_df, "ownership")
infra_ownership_intervals = infra_ownership_intervals.select(
        "asset_id",
        "ownership_phase_id",
        "ownership_milestone_type_id",
        "ownership_milestone_details_id",
        "active_from_date",
        "active_to_date"
    )

# COMMAND ----------

# DBTITLE 1,Remove accidental duplicates
invp_dim_df = invp_dim_raw.dropDuplicates(["investment_portfolio_id", "fund_core_id"])
invp_bridge_df = invp_bridge_raw.dropDuplicates(["investment_portfolio_id", "invp_version_id"])
invp_ver_df = invp_ver_raw.dropDuplicates(["invp_version_id", "active_from_date", "active_to_date"])

# Remove accidental mapping duplicates for the same (asset_id, investment_portfolio_id, invp_version_id, ts)
w_map = (
    W.partitionBy("asset_id","investment_portfolio_id","active_from_date","active_to_date") \
              .orderBy(F.lit(1))
)
asst_invp_raw = (
    asst_invp_raw
    .withColumn("_rn", F.row_number().over(w_map))
    .where(F.col("_rn")==1)
    .drop("_rn")
)

# COMMAND ----------

# Fill null active_to_date with date in far future
rels  = full_rels_hist_derived.withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))
a_i = asst_invp_raw.withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))
invp_ver_df = invp_ver_df.withColumn("active_to_date", F.coalesce(F.col("active_to_date"), FAR_FUTURE_TS))

asst_infra_lf = infra_lifecycle_intervals.select(
    "asset_id", "lifecycle_milestone_details_id", "lifecycle_milestone_type_id", "lifecycle_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_plat_lf = plat_lifecycle_derived.select(
    "asset_id", "lifecycle_milestone_details_id", "lifecycle_milestone_type_id", "lifecycle_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_infra_own = infra_ownership_intervals.select(
    "asset_id", "ownership_milestone_details_id", "ownership_milestone_type_id", "ownership_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_plat_own = plat_ownership_derived.select(
    "asset_id", "ownership_milestone_details_id", "ownership_milestone_type_id", "ownership_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

lfe = asst_infra_lf.union(asst_plat_lf)
own = asst_infra_own.union(asst_plat_own)

# Prefixes for each dataset
rel_pfx = "rels_"
a_i_pfx = "a_i_"
asst_pfx = "asst_"
invp_ver_pfx = "invp_ver_"
lfe_pfx = "lfe_"
own_pfx = "own_"


# Prefix columns (as each dataframe can have identical column headers)
rels = rels.select([F.col(c).alias(f"{rel_pfx}{c}") for c in rels.columns])
a_i = a_i.select([F.col(c).alias(f"{a_i_pfx}{c}") for c in a_i.columns])
invp_ver_df = invp_ver_df.select([F.col(c).alias(f"{invp_ver_pfx}{c}") for c in invp_ver_df.columns])
lfe = lfe.select([F.col(c).alias(f"{lfe_pfx}{c}") for c in lfe.columns])
own = own.select([F.col(c).alias(f"{own_pfx}{c}") for c in own.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC # Sparse, event-driven `calendarisation`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anchor

# COMMAND ----------

def overlap(left_from, left_to, right_from, right_to):
    # interval A [from,to) overlaps B [from,to)  <=> A.from < B.to AND B.from < A.to
    return (left_from < right_to) & (right_from < left_to)

# A: |------|        |------|
# B:    |------|  or  |------|      ✅ overlap --> keep

# A: |------|
# B:           |------|              ❌ no overlap --> drop

path_anchored = (
    rels
    .select(
        F.col(f"{rel_pfx}fund_core_id").alias("fund_core_id"),
        F.col(f"{rel_pfx}full_rel_id").alias("full_rel_id"),
        F.col(f"{rel_pfx}company_chain_id").alias("company_chain_id"),
        F.col(f"{rel_pfx}ultimate_parent_id").alias("ultimate_parent_id"),
        F.col(f"{rel_pfx}ultimate_child_id").alias("ultimate_child_id"),
        F.col(f"{rel_pfx}active_from_date").alias("rel_from"),
        F.col(f"{rel_pfx}active_to_date").alias("rel_to"),
        F.col(f"{rel_pfx}pairs_with_ownership").alias("pairs_with_ownership"),
        F.col(f"{rel_pfx}path_fraction_rounded").alias("ownership")
    )
    .join(F.broadcast(assetco_to_asset),
          F.col("ultimate_child_id")==F.col("assetco_core_id"), "inner")
    .select(
        "fund_core_id","full_rel_id","company_chain_id","ultimate_parent_id","ultimate_child_id",
        "assetco_core_id","asset_id",
        "rel_from","rel_to","pairs_with_ownership","ownership"
    )
)

ai_map = (
    a_i.select(
        F.col(f"{a_i_pfx}asset_id").alias("asset_id"),
        F.col(f"{a_i_pfx}investment_portfolio_id").alias("investment_portfolio_id"),
        F.col(f"{a_i_pfx}active_from_date").alias("ai_from"),
        F.col(f"{a_i_pfx}active_to_date").alias("ai_to"),
    )
)

invp_ver_map = (
    invp_ver_df.select(
        F.col(f"{invp_ver_pfx}invp_version_id").alias(f"{invp_ver_pfx}invp_version_id"),
        F.col(f"{invp_ver_pfx}active_from_date").alias(f"{invp_ver_pfx}active_from_date"),
        F.col(f"{invp_ver_pfx}active_to_date").alias(f"{invp_ver_pfx}active_to_date"),
    )
)

# COMMAND ----------

r = path_anchored.select(
    "fund_core_id","full_rel_id","company_chain_id",
    "ultimate_parent_id","ultimate_child_id",
    "assetco_core_id","asset_id",
    "rel_from","rel_to","ownership","pairs_with_ownership"
).alias("r")

m_all = ai_map.select(
    F.col("asset_id").alias("m_asset_id"),
    F.col("investment_portfolio_id").alias("m_investment_portfolio_id"),
    F.col("ai_from").alias("m_ai_from"),
    F.col("ai_to").alias("m_ai_to"),
).alias("m")

pf_all = (
    invp_dim_df.alias("dim")
    .join(
        broadcast(invp_bridge_df).alias("brg"), 
        on="investment_portfolio_id", 
        how="left"
    )
    .select(
        F.col("investment_portfolio_id").alias("pf_investment_portfolio_id"),
        F.col("brg.invp_version_id").alias("pf_invp_version_id"),
        F.col("dim.fund_core_id").alias("pf_fund_core_id"),
    )
    .alias("pf")
)

# Key-only join: r -> m (asset_id only), then m -> pf (portfolio/version only)
universe = (
    r
    .join(broadcast(m_all), F.col("r.asset_id") == F.col("m.m_asset_id"), "left")
    .join(
        broadcast(pf_all),
        (F.col("m.m_investment_portfolio_id") == F.col("pf.pf_investment_portfolio_id")),
        "left"
    )
    # Enforce circular correctness: portfolio/version must map back to SAME fund as relationship chain
    .withColumn("pf_matches_chain_fund", F.col("pf.pf_fund_core_id") == F.col("r.fund_core_id"))
    .where(F.col("pf_matches_chain_fund") | F.col("pf.pf_fund_core_id").isNull())
)

# COMMAND ----------

# DBTITLE 1,Standardise column names from universe
anchor_base = (
    universe
    .select(
        F.col("fund_core_id"),
        F.col("full_rel_id"),
        F.col("company_chain_id"),
        F.col("ultimate_parent_id"),
        F.col("ultimate_child_id"),
        F.col("assetco_core_id"),
        F.col("asset_id"),
        F.col("m_investment_portfolio_id").alias("investment_portfolio_id"),
        F.col("pf_invp_version_id").alias("invp_version_id"),
    )
    .dropDuplicates()
).cache()

if anchor_base.isEmpty():
    print("Warning: anchor_base is empty.")

ANCHOR_KEYS = [
    "asset_id",
    "assetco_core_id",
    "fund_core_id",
    "investment_portfolio_id",
    "invp_version_id",
    "full_rel_id",
    "company_chain_id",
    "ultimate_parent_id",
    "ultimate_child_id"
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Boundaries

# COMMAND ----------

def mk_bounds_from_to(anchor_df, join_df, join_cond, from_col, to_col, source_name):
    """Join anchor keys to some dataset and emit boundary events from from/to columns."""
    j = anchor_df.alias("a").join(join_df.alias("d"), join_cond, "left")

    b_from = (
        j.select(*[F.col(f"a.{c}").alias(c) for c in ANCHOR_KEYS],
                 F.col(from_col).cast("timestamp").alias("boundary"))
         .where("boundary is not null")
         .withColumn("source", F.lit(source_name + "_from"))
    )

    b_to = (
        j.select(*[F.col(f"a.{c}").alias(c) for c in ANCHOR_KEYS],
                 F.col(to_col).cast("timestamp").alias("boundary"))
         .where("boundary is not null")
         .withColumn("source", F.lit(source_name + "_to"))
    )

    return b_from.unionByName(b_to).dropDuplicates(ANCHOR_KEYS + ["boundary", "source"])


# -------------------------------
# 1) Relationship boundaries (from path_anchored intervals)
# path_anchored already has rel_from/rel_to + keys except portfolio/version
# We join rel intervals onto every anchor row that shares the same rel identity (fund+full_rel+asset, etc.)
# -------------------------------
rels_for_bounds = path_anchored.select(
    "fund_core_id","full_rel_id","company_chain_id",
    "ultimate_parent_id","ultimate_child_id",
    "assetco_core_id","asset_id",
    "rel_from","rel_to"
).dropDuplicates()

rel_bounds = mk_bounds_from_to(
    anchor_base,
    rels_for_bounds,
    join_cond=(
        (F.col("a.fund_core_id") == F.col("d.fund_core_id")) &
        (F.col("a.full_rel_id") == F.col("d.full_rel_id")) &
        (F.col("a.company_chain_id") == F.col("d.company_chain_id")) &
        (F.col("a.ultimate_parent_id") == F.col("d.ultimate_parent_id")) &
        (F.col("a.ultimate_child_id") == F.col("d.ultimate_child_id")) &
        (F.col("a.assetco_core_id") == F.col("d.assetco_core_id")) &
        (F.col("a.asset_id") == F.col("d.asset_id"))
    ),
    from_col="d.rel_from",
    to_col="d.rel_to",
    source_name="rels"
)

# -------------------------------
# 2) Asset -> Portfolio mapping boundaries (from a_i / ai_map)
# -------------------------------
ai_bounds = mk_bounds_from_to(
    anchor_base,
    ai_map,  # ai_map is already non-prefixed with ai_from/ai_to etc
    join_cond=(
        (F.col("a.asset_id") == F.col("d.asset_id")) &
        (F.col("a.investment_portfolio_id") == F.col("d.investment_portfolio_id"))
    ),
    from_col="d.ai_from",
    to_col="d.ai_to",
    source_name="a_i"
)

# -------------------------------
# 3) Portfolio version
# -------------------------------
pf_bounds = mk_bounds_from_to(
    anchor_base,
    invp_ver_map,
    join_cond=(
        (F.col("a.invp_version_id") == F.col(f"d.{invp_ver_pfx}invp_version_id"))
    ),
    from_col=f"d.{invp_ver_pfx}active_from_date",
    to_col=f"d.{invp_ver_pfx}active_to_date",
    source_name="pf"
)

# -------------------------------
# 4) Lifecycle & ownership milestone boundaries (from lfe / own)
# -------------------------------
lfe_bounds = mk_bounds_from_to(
    anchor_base,
    lfe,
    join_cond=(F.col("a.asset_id") == F.col(f"d.{lfe_pfx}asset_id")),
    from_col=f"d.{lfe_pfx}active_from_date",
    to_col=f"d.{lfe_pfx}active_to_date",
    source_name="lifecycle"
)

own_bounds = mk_bounds_from_to(
    anchor_base,
    own,
    join_cond=(F.col("a.asset_id") == F.col(f"d.{own_pfx}asset_id")),
    from_col=f"d.{own_pfx}active_from_date",
    to_col=f"d.{own_pfx}active_to_date",
    source_name="ownership"
)

# Create a "Today" boundary for every asset in the anchor
today_bounds = (
    anchor_base.select(*ANCHOR_KEYS)
    .withColumn("boundary", F.to_timestamp(F.current_date()))
    .withColumn("source", F.lit("today_boundary"))
    .distinct()
)

# Union them all
boundary_catalog = (
    rel_bounds
    .unionByName(ai_bounds)
    .unionByName(pf_bounds)
    .unionByName(lfe_bounds)
    .unionByName(own_bounds)
    .unionByName(today_bounds)
)

all_bounds = boundary_catalog.select(*ANCHOR_KEYS, "boundary").where("boundary is not null")

# Build minimal, gapless segments per anchor key
boundaries = (
    all_bounds
    .groupBy(*ANCHOR_KEYS)
    .agg(F.array_sort(F.array_distinct(F.collect_list("boundary"))).alias("b"))
    .withColumn(
        "segments",
        F.expr("""
          transform(sequence(1, size(b)-1),
                    i -> struct(b[i-1] as start, b[i] as end))
        """)
    )
    .select(*ANCHOR_KEYS, F.explode("segments").alias("seg"))
    .select(*ANCHOR_KEYS,
            F.col("seg.start").cast("timestamp").alias("start"),
            F.col("seg.end").cast("timestamp").alias("end"))
)

GB_COLS = ANCHOR_KEYS + ["start", "end"]

# COMMAND ----------

# Relationship windows come from path_anchored (not anchor_base now)
rel_windows = path_anchored.select(
    "fund_core_id","full_rel_id","company_chain_id",
    "ultimate_parent_id","ultimate_child_id",
    "assetco_core_id","asset_id",
    "rel_from","rel_to","ownership","pairs_with_ownership"
).dropDuplicates()

# Attach relationship coverage (has_relpath) to each segment
seg_rel = (
    boundaries.alias("s")
    .join(
        rel_windows.alias("r"),
        (F.col("s.fund_core_id")==F.col("r.fund_core_id")) &
        (F.col("s.full_rel_id")==F.col("r.full_rel_id")) &
        (F.col("s.company_chain_id")==F.col("r.company_chain_id")) &
        (F.col("s.ultimate_parent_id")==F.col("r.ultimate_parent_id")) &
        (F.col("s.ultimate_child_id")==F.col("r.ultimate_child_id")) &
        (F.col("s.assetco_core_id")==F.col("r.assetco_core_id")) &
        (F.col("s.asset_id")==F.col("r.asset_id")) &
        (F.col("s.start") >= F.col("r.rel_from")) &
        (F.col("s.end")   <= F.col("r.rel_to")),
        "left"
    )
    .select(
        *[F.col(f"s.{c}").alias(c) for c in GB_COLS],
        F.col("r.ownership").alias("ownership"),
        F.col("r.pairs_with_ownership").alias("pairs_with_ownership"),
        F.col("r.full_rel_id").isNotNull().alias("hit_rel")
    )
    .groupBy(*GB_COLS)
    .agg(
        F.first("ownership", ignorenulls=True).alias("ownership"),
        F.first("pairs_with_ownership", ignorenulls=True).alias("pairs_with_ownership"),
        F.max("hit_rel").alias("has_relpath")
    )
)

# Mapping coverage: segment is within a_i window for that asset+portfolio+version
seg_map = (
    seg_rel.alias("s")
    .join(
        ai_map.alias("m"),
        (F.col("s.asset_id") == F.col("m.asset_id")) &
        (F.col("s.investment_portfolio_id") == F.col("m.investment_portfolio_id")) &
        (F.col("s.start") >= F.col("m.ai_from")) &
        (F.col("s.end")   <= F.col("m.ai_to")),
        "left"
    )
    .select(
        *[F.col(f"s.{c}").alias(c) for c in GB_COLS],
        "ownership","pairs_with_ownership","has_relpath",
        F.col("m.asset_id").isNotNull().alias("hit_ai")
    )
    .groupBy(*GB_COLS)
    .agg(
        F.first("ownership", ignorenulls=True).alias("ownership"),
        F.first("pairs_with_ownership", ignorenulls=True).alias("pairs_with_ownership"),
        F.first("has_relpath", ignorenulls=True).alias("has_relpath"),
        F.max("hit_ai").alias("has_asst_invp_map")
    )
)

# PF coverage evaluated across Dim, Bridge, and Version independently
seg_invp = (
    seg_map.alias("s")
    .join(
        broadcast(invp_dim_df).alias("pd"),
        (F.col("s.investment_portfolio_id") == F.col("pd.investment_portfolio_id")) &
        (F.col("s.fund_core_id") == F.col("pd.fund_core_id")),
        "left"
    )
    .join(
        broadcast(invp_bridge_df).alias("pb"),
        (F.col("s.investment_portfolio_id") == F.col("pb.investment_portfolio_id")) &
        (F.col("s.invp_version_id") == F.col("pb.invp_version_id")),
        "left"
    )
    .join(
        broadcast(invp_ver_df).alias("pv"),
        (F.col("s.invp_version_id") == F.col(f"pv.{invp_ver_pfx}invp_version_id")) &
        (F.col("s.start") >= F.col(f"pv.{invp_ver_pfx}active_from_date")) &
        (F.col("s.end")   <= F.col(f"pv.{invp_ver_pfx}active_to_date")),
        "left"
    )
    .select(
        *[F.col(f"s.{c}").alias(c) for c in GB_COLS],
        "ownership","pairs_with_ownership","has_relpath","has_asst_invp_map",
        F.col("pd.investment_portfolio_id").isNotNull().alias("hit_dim"),
        F.col("pb.invp_version_id").isNotNull().alias("hit_brg"),
        F.col(f"pv.{invp_ver_pfx}invp_version_id").isNotNull().alias("hit_ver")
    )
    .groupBy(*GB_COLS)
    .agg(
        F.first("ownership", ignorenulls=True).alias("ownership"),
        F.first("pairs_with_ownership", ignorenulls=True).alias("pairs_with_ownership"),
        F.first("has_relpath", ignorenulls=True).alias("has_relpath"),
        F.first("has_asst_invp_map", ignorenulls=True).alias("has_asst_invp_map"),
        F.max("hit_dim").alias("has_invp_dim"),
        F.max("hit_brg").alias("has_invp_bridge"),
        F.max("hit_ver").alias("has_invp_version")
    )
)

seg_comp = (
    seg_invp.alias("s")
    # Lifecycle milestone covering the whole segment
    .join(
        lfe.alias("l"),
        (F.col("s.asset_id") == F.col(f"l.{lfe_pfx}asset_id")) &
        (F.col("s.start")    >= F.col(f"l.{lfe_pfx}active_from_date")) &
        (F.col("s.end")      <= F.col(f"l.{lfe_pfx}active_to_date")),
        "left"
    )
    # Ownership milestone covering the whole segment
    .join(
        own.alias("o"),
        (F.col("s.asset_id") == F.col(f"o.{own_pfx}asset_id")) &
        (F.col("s.start")    >= F.col(f"o.{own_pfx}active_from_date")) &
        (F.col("s.end")      <= F.col(f"o.{own_pfx}active_to_date")),
        "left"
    )
    .select(
        *[F.col(f"s.{c}").alias(c) for c in GB_COLS],

        # pass-through from seg_invp
        F.col("s.ownership").alias("ownership"),
        F.col("s.pairs_with_ownership").alias("pairs_with_ownership"),
        F.col("s.has_relpath").alias("has_relpath"),
        F.col("s.has_asst_invp_map").alias("has_asst_invp_map"),
        F.col("s.has_invp_dim").alias("has_invp_dim"),
        F.col("s.has_invp_bridge").alias("has_invp_bridge"),
        F.col("s.has_invp_version").alias("has_invp_version"),

        # lifecycle (IDs + descriptors)
        F.col(f"l.{lfe_pfx}lifecycle_milestone_details_id").alias("lifecycle_milestone_details_id"),
        F.col(f"l.{lfe_pfx}lifecycle_milestone_type_id").alias("lifecycle_milestone_type_id"),
        F.col(f"l.{lfe_pfx}lifecycle_phase_id").alias("lifecycle_phase_id"),

        # ownership (IDs + descriptors)
        F.col(f"o.{own_pfx}ownership_milestone_details_id").alias("ownership_milestone_details_id"),
        F.col(f"o.{own_pfx}ownership_milestone_type_id").alias("ownership_milestone_type_id"),
        F.col(f"o.{own_pfx}ownership_phase_id").alias("ownership_phase_id"),
    )
    # collapse any duplicates deterministically per segment
    .groupBy(*GB_COLS)
    .agg(
        F.first("ownership",              True).alias("ownership"),
        F.first("pairs_with_ownership",   True).alias("pairs_with_ownership"),
        F.first("has_relpath",            True).alias("has_relpath"),
        F.first("has_asst_invp_map",         True).alias("has_asst_invp_map"),
        F.first("has_invp_dim",        True).alias("has_invp_dim"),
        F.first("has_invp_bridge",     True).alias("has_invp_bridge"),
        F.first("has_invp_version",    True).alias("has_invp_version"),

        # lifecycle
        F.first("lifecycle_milestone_details_id",        True).alias("lifecycle_milestone_details_id"),
        F.first("lifecycle_milestone_type_id", True).alias("lifecycle_milestone_type_id"),
        F.first("lifecycle_phase_id",          True).alias("lifecycle_phase_id"),

        # ownership
        F.first("ownership_milestone_details_id",        True).alias("ownership_milestone_details_id"),
        F.first("ownership_milestone_type_id", True).alias("ownership_milestone_type_id"),
        F.first("ownership_phase_id",          True).alias("ownership_phase_id"),
    )
    .withColumn("has_lifecycle_milestone", F.coalesce(F.size("lifecycle_milestone_details_id") > 0, F.lit(False)))
    .withColumn("has_ownership_milestone", F.coalesce(F.size("ownership_milestone_details_id") > 0, F.lit(False)))
)

df_final = seg_comp

# COMMAND ----------

# PROVENANCE: use the boundary_catalog already built above (the one made from rel_bounds/ai_bounds/pf_bounds/lfe_bounds/own_bounds)
boundary_summary = (
    boundary_catalog
    .select(*ANCHOR_KEYS, "boundary", "source")
    .groupBy(*(ANCHOR_KEYS + ["boundary"]))
    .agg(F.sort_array(F.collect_set("source")).alias("opened_by_sources"))
)

segment_open_events = (
    df_final
    .withColumn("segment_start_ts", F.col("start").cast("timestamp"))
    .join(
        boundary_summary.withColumnRenamed("boundary", "segment_start_ts"),
        on=(ANCHOR_KEYS + ["segment_start_ts"]),
        how="left"
    )
    .drop("segment_start_ts")
    .withColumn("opened_by_sources", F.coalesce("opened_by_sources", F.array()))
    .withColumnRenamed("pairs_with_ownership", "company_chain_ownership_id")
)

# COMMAND ----------

# DBTITLE 1,Cell 38
# --- Step 7: Apply business rules (Lock Box, Divested Phase, Actuals Stretch) and flags ---

# 1. Identify Today's date for boundary logic
TODAY_TS = F.to_timestamp(F.current_date())

# 2. Identify specific states
is_divested = (F.col("ownership_phase_id") == F.lit("OWNR_PHSE_1005"))

# Lock Box rule (OWNR_MILE_1002) logic
lock_box = (
    own
    .select(
        F.col(f"{own_pfx}asset_id").alias("asset_id"),
        F.explode(f"{own_pfx}ownership_milestone_details_id").alias("ms")
    )
    .where(F.col("ms.milestone_id") == F.lit("OWNR_MILE_1002"))
    .groupBy("asset_id")
    .agg(F.min("ms.milestone_date").alias("lock_box_ts"))
)

segment_open_events = segment_open_events.join(F.broadcast(lock_box), on="asset_id", how="left")

# Completion rule (OWNR_MILE_1006) logic — once legal transfer is complete, Lock Box override is skipped
completion = (
    own
    .select(
        F.col(f"{own_pfx}asset_id").alias("asset_id"),
        F.explode(f"{own_pfx}ownership_milestone_details_id").alias("ms")
    )
    .where(F.col("ms.milestone_id") == F.lit("OWNR_MILE_1006"))
    .groupBy("asset_id")
    .agg(F.min("ms.milestone_date").alias("completion_ts"))
)

segment_open_events = segment_open_events.join(F.broadcast(completion), on="asset_id", how="left")

is_post_lock_box_date = (
    F.exists(F.col("ownership_milestone_details_id"), lambda x: x.milestone_id == F.lit("OWNR_MILE_1002")) |
    (F.col("lock_box_ts").isNotNull() & (F.col("start") >= F.col("lock_box_ts")))
)

has_reached_completion = (
    F.exists(F.col("ownership_milestone_details_id"), lambda x: x.milestone_id == F.lit("OWNR_MILE_1006")) |
    (F.col("completion_ts").isNotNull() & (F.col("start") >= F.col("completion_ts")))
)

# 3. "Stretch" Actuals forward to replace Forecasts in the past
# We define a window to look back at preceding segments for the same asset
w_past = W.partitionBy("asset_id").orderBy("start").rowsBetween(W.unboundedPreceding, W.currentRow)

# Check for existing Actuals before stretching
has_actual_lifecycle = (F.col("lifecycle_milestone_type_id") == F.lit("MILE_TYPE_1002"))
has_actual_ownership = (F.col("ownership_milestone_type_id") == F.lit("MILE_TYPE_1002"))

segment_open_events = (
    segment_open_events
    # Find the last known Actual values in the timeline
    .withColumn("_last_actual_l_phase", F.last(F.when(has_actual_lifecycle, F.col("lifecycle_phase_id")), True).over(w_past))
    .withColumn("_last_actual_l_type", F.last(F.when(has_actual_lifecycle, F.col("lifecycle_milestone_type_id")), True).over(w_past))
    .withColumn("_last_actual_o_phase", F.last(F.when(has_actual_ownership, F.col("ownership_phase_id")), True).over(w_past))
    .withColumn("_last_actual_o_type", F.last(F.when(has_actual_ownership, F.col("ownership_milestone_type_id")), True).over(w_past))
    
    # OVERRIDE: If segment ends today or earlier AND is a Forecast, but a preceding Actual exists -> Stretch the Actual
    .withColumn("lifecycle_phase_id", 
        F.when((F.col("end") <= TODAY_TS) & (~has_actual_lifecycle) & F.col("_last_actual_l_phase").isNotNull(), F.col("_last_actual_l_phase"))
         .otherwise(F.col("lifecycle_phase_id")))
    .withColumn("lifecycle_milestone_type_id", 
        F.when((F.col("end") <= TODAY_TS) & (~has_actual_lifecycle) & F.col("_last_actual_l_type").isNotNull(), F.col("_last_actual_l_type"))
         .otherwise(F.col("lifecycle_milestone_type_id")))
    .withColumn("ownership_phase_id", 
        F.when((F.col("end") <= TODAY_TS) & (~has_actual_ownership) & F.col("_last_actual_o_phase").isNotNull(), F.col("_last_actual_o_phase"))
         .otherwise(F.col("ownership_phase_id")))
    .withColumn("ownership_milestone_type_id", 
        F.when((F.col("end") <= TODAY_TS) & (~has_actual_ownership) & F.col("_last_actual_o_type").isNotNull(), F.col("_last_actual_o_type"))
         .otherwise(F.col("ownership_milestone_type_id")))
)

# 4. Final Validity Gates
# Re-evaluate Actual flags after the stretch
has_actual_lifecycle_final = (F.col("lifecycle_milestone_type_id") == F.lit("MILE_TYPE_1002"))
has_actual_ownership_final = (F.col("ownership_milestone_type_id") == F.lit("MILE_TYPE_1002"))

# If it's still a forecast for a past date after the stretch (i.e. no preceding actual), it's invalid
is_invalid_forecast = (
    (F.col("end") <= TODAY_TS) & 
    (~has_actual_lifecycle_final | ~has_actual_ownership_final) & 
    (~is_divested)
)

gates_standard_good = (
    F.col("has_relpath") &
    F.col("has_asst_invp_map") &
    F.col("has_invp_dim") &
    F.col("has_invp_bridge") &
    F.col("has_invp_version") &
    F.col("has_lifecycle_milestone") &
    F.col("has_ownership_milestone") &
    (~is_invalid_forecast)
)

gates_divested_good = (
    is_divested & 
    F.col("investment_portfolio_id").isNotNull() & 
    F.col("invp_version_id").isNotNull()
)

is_gate_passed = gates_standard_good | gates_divested_good

# 5. Calculate Effective Ownership and Questionable Flag
segment_open_events = (
    segment_open_events
    .withColumn(
        "effective_path_ownership",
        F.when(is_post_lock_box_date & ~has_reached_completion,
            F.when(F.col("ownership").isNotNull() & (F.col("ownership") != F.lit(0.0)), F.col("ownership"))
             .otherwise(F.lit(1.0))  # Default Lock Box to 100% when no non-zero ownership is set
        )
         .when(is_gate_passed & F.col("ownership").isNotNull(), F.col("ownership"))
         .otherwise(F.lit(0.0))
    )
    .withColumn("_ownership_coalesced", F.coalesce(F.col("ownership"), F.lit(0.0)).cast("double"))
    .withColumn("_effective_coalesced", F.coalesce(F.col("effective_path_ownership"), F.lit(0.0)).cast("double"))
    .withColumn("equal_ownerships", F.col("_ownership_coalesced") == F.col("_effective_coalesced"))
    .withColumn(
        "questionable",
        F.when(
            is_gate_passed & (F.col("equal_ownerships") | is_post_lock_box_date),
            F.lit(False)
        )
        .otherwise(F.lit(True))
    )
    .drop("_ownership_coalesced", "_effective_coalesced", 
          "_last_actual_l_phase", "_last_actual_l_type", "_last_actual_o_phase", "_last_actual_o_type")
    .select(
        "opened_by_sources", "start", "end", "asset_id", "lifecycle_milestone_details_id", 
        "lifecycle_milestone_type_id", "lifecycle_phase_id", "ownership_milestone_details_id", 
        "ownership_milestone_type_id", "ownership_phase_id", "investment_portfolio_id",
        "invp_version_id", "fund_core_id", "full_rel_id", "company_chain_id", 
        "ultimate_parent_id", "ultimate_child_id", "ownership", "company_chain_ownership_id", 
        "effective_path_ownership", "equal_ownerships", "has_relpath", "has_asst_invp_map", 
        "has_invp_dim", "has_invp_bridge", "has_invp_version", "has_lifecycle_milestone", 
        "has_ownership_milestone", "questionable"
    )
)

# Safely store checkpoint data ON persistent cloud storage (can't do localChecpoint with availability: SPOT_WITH_FALLBACK & autoscalilng in the DABs config)

checkpoint_path = f"dbfs:/tmp/checkpoints/xio/{CATALOG}/{DATABASE}/{TABLE}/{BUNDLE_TARGET}"

dbutils.fs.rm(checkpoint_path, recurse=True)
spark.sparkContext.setCheckpointDir(checkpoint_path)
segment_open_events = segment_open_events.checkpoint() # checkpoint() is 100% immune to Spot instance preemption


# COMMAND ----------

# partition at your natural grain, EXCLUDING time
NATURAL_GRAIN_PARTITION = ["asset_id","fund_core_id","investment_portfolio_id","invp_version_id","full_rel_id","company_chain_id"]
EXPECTED_KEY = NATURAL_GRAIN_PARTITION + ["start","end"]

# state columns = all non-time columns you care about EXCEPT the group keys
STATE_COLS_ALL = [
    "asset_id",
    "lifecycle_milestone_details_id","lifecycle_milestone_type_id","lifecycle_phase_id",
    "ownership_milestone_details_id","ownership_milestone_type_id","ownership_phase_id",
    "investment_portfolio_id","invp_version_id","fund_core_id",
    "full_rel_id","company_chain_id", "ultimate_parent_id","ultimate_child_id",
    "ownership","company_chain_ownership_id",
    "effective_path_ownership","equal_ownerships", "has_relpath", "has_asst_invp_map",
    "has_invp_dim", "has_invp_bridge", "has_invp_version",
    "has_lifecycle_milestone","has_ownership_milestone",
    "questionable"
]

STATE_COLS = [c for c in STATE_COLS_ALL if c not in NATURAL_GRAIN_PARTITION]

w = W.partitionBy(*NATURAL_GRAIN_PARTITION).orderBy("start")

df = segment_open_events

# compare to previous row
prev_end = F.lag("end").over(w)

# build a boolean "same state as previous" check
same_state = F.lit(True)
for c in STATE_COLS:
    same_state = same_state & (F.col(c).eqNullSafe(F.lag(c).over(w)))

# start a new group if:
# - first row
# - gap/overlap (prev_end != start)
# - or any state column changed
new_group = (
    prev_end.isNull() |
    (prev_end != F.col("start")) |
    (~same_state)
)

df2 = (
    df
    .withColumn("_new_group", F.when(new_group, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("_grp", F.sum("_new_group").over(w))
)

# collapse each group to one row
collapsed = (
    df2
    .groupBy(*NATURAL_GRAIN_PARTITION, "_grp")
    .agg(
        F.min("start").alias("start"),
        F.max("end").alias("end"),
        # merge provenance instead of picking one
        F.sort_array(F.array_distinct(F.flatten(F.collect_list("opened_by_sources")))).alias("opened_by_sources"),
        *[F.first(c, True).alias(c) for c in STATE_COLS]  # state is constant within grp by construction
    )
    .drop("_grp")
)

# Attach the deduplicated Metadata Arrays
collapsed = (
    collapsed
    .join(F.broadcast(asset_metadata_df), on="asset_id", how="left")
    .withColumn("country_core_id", F.coalesce(F.col("country_core_id"), empty_str_array))
    .withColumn("country_groups_id", F.coalesce(F.col("country_groups_id"), empty_str_array))
    .withColumn("technology_name_id", F.coalesce(F.col("technology_name_id"), empty_str_array))
    .withColumn("technology_core_id", F.coalesce(F.col("technology_core_id"), empty_str_array))
    .withColumn("technology_grouping_tier_1_id", F.coalesce(F.col("technology_grouping_tier_1_id"), empty_str_array))
    .withColumn("technology_grouping_tier_2_id", F.coalesce(F.col("technology_grouping_tier_2_id"), empty_str_array))
)

FINAL_COLUMN_ORDER = [
    # 1. Core Entity IDs
    "fund_core_id",
    "investment_portfolio_id",
    "invp_version_id",
    "asset_id",

    # 2. Asset Metadata IDs
    "country_core_id",
    "country_groups_id",
    "technology_name_id",
    "technology_core_id",
    "technology_grouping_tier_1_id",
    "technology_grouping_tier_2_id",

    # 3. Relationship / Chain IDs
    "full_rel_id",
    "company_chain_id",
    "ultimate_parent_id",
    "ultimate_child_id",

    # 4. Temporal Boundaries
    "start",
    "end",

    # 5. Core Metrics & Path Values
    "ownership",
    "effective_path_ownership",
    "company_chain_ownership_id", 
    "equal_ownerships",

    # 6. Lifecycle Attributes
    "lifecycle_phase_id",
    "lifecycle_milestone_type_id",
    "lifecycle_milestone_details_id",

    # 7. Asset Ownership Attributes
    "ownership_phase_id",
    "ownership_milestone_type_id",
    "ownership_milestone_details_id",

    # 8. Provenance & Audit Flags
    "has_relpath",
    "has_asst_invp_map",
    "has_invp_dim",
    "has_invp_bridge",
    "has_invp_version",
    "has_lifecycle_milestone",
    "has_ownership_milestone",
    "questionable",
    "opened_by_sources"
]

# Apply the clean ordering and set 9999-12-31 back to NULL
collapsed = (
    collapsed
    .select(*FINAL_COLUMN_ORDER)
    .withColumn(
        "end", 
        F.when(F.col("end") == FAR_FUTURE_TS, F.lit(None).cast("timestamp"))
         .otherwise(F.col("end"))
    )
)

# COMMAND ----------

dups_out = (
  collapsed
    .groupBy(*EXPECTED_KEY)
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)

if not dups_out.isEmpty():
    display(dups_out)
    raise ValueError("Duplicate rows detected at expected output grain (including full_rel_id).")

else:
    collapsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FULL_TABLE_NAME)
    anchor_base.unpersist() # This frees up the cluster's memory

print(f'Written to {FULL_TABLE_NAME}')

# COMMAND ----------

