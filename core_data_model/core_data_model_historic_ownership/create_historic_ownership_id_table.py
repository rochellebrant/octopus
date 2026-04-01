# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Code Description
# MAGIC We turn multiple time-varying datasets (fund→company ownership chains, asset↔portfolio mappings, portfolio version / fund validity, lifecycle milestones, and ownership milestones) into a single consistent history by:
# MAGIC
# MAGIC 1. Building ownership chains from fund to asset companies and deriving path-level ownership.
# MAGIC 2. Converting all sources into `[from, to)` intervals (capping open-ended dates to a far-future timestamp).
# MAGIC 3. Anchoring everything on a common key (fund, portfolio, version, asset, company chain).
# MAGIC 4. Building all change boundaries, then slicing time into minimal segments where nothing relevant changes.
# MAGIC 5. Annotating each segment with state from every dataset, including Lock Box rules and provenance.
# MAGIC 6. Flagging “questionable” segments where the data is incomplete or inconsistent.
# MAGIC 7. Collapsing adjacent identical segments to avoid unnecessary splits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1. Business Context – What are we trying to achieve and why?
# MAGIC
# MAGIC ### The problem we are solving
# MAGIC When ownership of an asset changes, we record it in several “core” tables. Various events impact ownership of assets, including:
# MAGIC
# MAGIC - Changes in the fund ↔ company ownership chain
# MAGIC - Assets moving between portfolios
# MAGIC - Portfolio versions changing over time
# MAGIC - Asset lifecycle milestones (e.g. first transaction, COD, etc.)
# MAGIC - Asset ownership milestones (e.g. Lock Box)
# MAGIC - Business rules that override raw ownership (e.g. Lock Box → 100% ownership)
# MAGIC
# MAGIC Limitations of the raw “core” tables:
# MAGIC
# MAGIC - They exist independently.
# MAGIC - Each has its own validity date logic.
# MAGIC - Their active windows often do **not** align cleanly.
# MAGIC - Simple “as-at date” joins produce incorrect or misleading ownerships.
# MAGIC
# MAGIC We need a single, time-aware view of:
# MAGIC
# MAGIC > **“Who owns what, when, and under which rules?”**
# MAGIC
# MAGIC This must work for both infrastructure assets and platform assets.
# MAGIC
# MAGIC ### What this table enables
# MAGIC The `historic_ownership` / `historic_ownership_id` tables allow us to:
# MAGIC
# MAGIC - Answer ownership at any point in time
# MAGIC - Understand *why* ownership changed (chain, mapping, milestones, rules)
# MAGIC - Trace how ownership was calculated (full provenance)
# MAGIC - Apply business rules consistently
# MAGIC - Flag data-quality risks (`questionable`)
# MAGIC
# MAGIC The table is designed to be:
# MAGIC
# MAGIC - **Analytically safe** (no hidden joins)
# MAGIC - **Explainable** (every segment has provenance and flags)
# MAGIC - **Reusable** across reporting, modelling, and audit use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2. Conceptual Model – How to think about the table
# MAGIC
# MAGIC ### Key mental model: “Time-segmented state”
# MAGIC Each row represents a **continuous time segment** where everything relevant is constant:
# MAGIC
# MAGIC - Same asset
# MAGIC - Same fund
# MAGIC - Same investment portfolio & version
# MAGIC - Same company chain (fund→…→assetCo)
# MAGIC - Same lifecycle state
# MAGIC - Same ownership milestone state
# MAGIC - Same ownership/Lock Box outcome
# MAGIC
# MAGIC If *anything* changes, a new row opens.
# MAGIC
# MAGIC ### Inclusive / exclusive time logic
# MAGIC All segments use `[start, end)`:
# MAGIC
# MAGIC - `start` is **inclusive**
# MAGIC - `end` is **exclusive**
# MAGIC
# MAGIC This avoids overlaps, double-counting, and “fencepost” issues when querying by timestamp.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3. High-level Overview – What the code does
# MAGIC
# MAGIC > There is a separate visual join diagram in Miro:  
# MAGIC > https://miro.com/app/board/uXjVJrMTUJQ=/
# MAGIC
# MAGIC We’re essentially doing a circular join on PKs/CKs/FKs and enforcing that their `active_from` / `active_to` windows overlap correctly.
# MAGIC
# MAGIC ## Step 1: Build ownership chains (fund → asset company)
# MAGIC
# MAGIC 1. Start from `bronze_company_parent_relationship_map` and join to:
# MAGIC    - `bronze_fund_dim_core` (fund top-level parent),
# MAGIC    - `bronze_asset_infra_dim_core` and `bronze_asset_platform_dim_core` (child companies that are assets/platforms).
# MAGIC 2. Filter to “full” relationships where:
# MAGIC    - Parent is an active fund company.
# MAGIC    - Child is an active asset or platform company.
# MAGIC    - Relationship and fund records are active (`END_AT IS NULL`).
# MAGIC 3. For each `rel_id`, parse the company chain from the `rel_id` pattern:
# MAGIC    - Split into components: `["COMP_1092","COMP_1092","COMP_1243","COMP_1244", ...]`.
# MAGIC    - Remove **consecutive** duplicates to get a clean chain: `["COMP_1092","COMP_1243","COMP_1244"]`.
# MAGIC 4. Turn each chain into **parent-child pairs**:
# MAGIC    - `["COMP_1092","COMP_1243"]`, `["COMP_1243","COMP_1244"]`, etc.
# MAGIC    - Create `pair_rel_id` like `COMP_1092.COMP_1243`.
# MAGIC 5. Collect the **distinct parent-child edges** used in any full chain and:
# MAGIC    - Look up their percentage and transaction windows.
# MAGIC    - Build per-edge intervals: `[active_from_date, active_to_date)`, using `transaction_date` and the next transaction.
# MAGIC    - Cap open-ended `active_to_date` to a far-future timestamp (`9999-12-31 23:59:59`).
# MAGIC
# MAGIC For each fund–rel_id–asset company chain and for each time window where all edges are simultaneously active:
# MAGIC
# MAGIC - We intersect all edge intervals to find minimal segments where **all pairs in the path are valid**.
# MAGIC - For each segment, we carry:
# MAGIC   - `pairs_with_ownership`: array describing each edge in the chain and its percentage.
# MAGIC   - `path_fraction_rounded`: product of edge percentages → raw chain ownership (0–1).
# MAGIC
# MAGIC Output (simplified example):
# MAGIC
# MAGIC - `company_chain_id` → `["COMP_1092","COMP_1243","COMP_1244"]`
# MAGIC - `pairs_with_ownership` / `company_chain_ownership_id`:
# MAGIC   ```json
# MAGIC   [
# MAGIC     {
# MAGIC       "pair_rel_id": "COMP_1092.COMP_1243",
# MAGIC       "percentage": 1.0,
# MAGIC       "parent_child_pair": ["COMP_1092","COMP_1243"]
# MAGIC     },
# MAGIC     {
# MAGIC       "pair_rel_id": "COMP_1243.COMP_1244",
# MAGIC       "percentage": 1.0,
# MAGIC       "parent_child_pair": ["COMP_1243","COMP_1244"]
# MAGIC     }
# MAGIC   ]
# MAGIC   ```
# MAGIC - `ownership` → product of edge percentages (raw chain ownership)
# MAGIC
# MAGIC This gives **fund→asset company** history, with explicit chain breakdown and raw ownership per time interval.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Convert everything into intervals
# MAGIC
# MAGIC Every source becomes a `[from, to)` interval table (with open-ended `to` capped to `9999-12-31 23:59:59`):
# MAGIC
# MAGIC Source                              | Interval represents
# MAGIC ----------------------------------- | -------------------
# MAGIC Fund→company relationships          | Relationship / chain validity
# MAGIC Asset ↔ Portfolio mapping           | Asset→portfolio mapping validity
# MAGIC Portfolio Version (and → Fund)      | Portfolio version validity and its fund assignment
# MAGIC Lifecycle milestones (infra + plat) | Lifecycle state over time
# MAGIC Ownership milestones (infra + plat) | Ownership phase over time
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - Infrastructure and platform assets are both supported:
# MAGIC   - Infra lifecycle/ownership comes from `silver_asset_lifecycle_fact` & `silver_asset_ownership_fact`.
# MAGIC   - Platform lifecycle / ownership intervals are derived from `bronze_asset_platform_dim_core` + relationship history.
# MAGIC - All `NULL` active-to dates are capped to the far-future timestamp.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Anchor all datasets together
# MAGIC
# MAGIC We define a **common anchor grain** for all segments:
# MAGIC
# MAGIC ```text
# MAGIC asset_id,
# MAGIC assetco_core_id,
# MAGIC fund_core_id,
# MAGIC investment_portfolio_id,
# MAGIC invp_version_id,
# MAGIC full_rel_id,
# MAGIC company_chain_id,
# MAGIC ultimate_parent_id,
# MAGIC ultimate_child_id
# MAGIC ```
# MAGIC
# MAGIC The anchoring logic ensures:
# MAGIC
# MAGIC - Asset→portfolio mapping (`asset_id`, `investment_portfolio_id`, `invp_version_id`) is valid.
# MAGIC - Portfolio version→fund mapping matches the **same** `fund_core_id` as the ownership chain (circular correctness).
# MAGIC - The fund→company ownership chain points to the same fund.
# MAGIC - All relevant intervals have the potential to overlap for that key combination.
# MAGIC
# MAGIC This defines the **maximal universe** of valid (fund, portfolio, version, asset, chain) combinations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Build all change boundaries
# MAGIC
# MAGIC For each anchor key, we collect **all start and end timestamps** from:
# MAGIC
# MAGIC - Relationship intervals (`rel_from`, `rel_to`)
# MAGIC - Asset ↔ portfolio mapping intervals (`ai_from`, `ai_to`)
# MAGIC - Portfolio version → fund intervals (`pf_from`, `pf_to`)
# MAGIC - Lifecycle intervals (`lifecycle_active_from`, `lifecycle_active_to`)
# MAGIC - Ownership intervals (`ownership_active_from`, `ownership_active_to`)
# MAGIC
# MAGIC We:
# MAGIC
# MAGIC 1. Emit boundary events from each dataset with a `source` label, e.g.:
# MAGIC    - `"rels_from"`, `"rels_to"`
# MAGIC    - `"a_i_from"`, `"a_i_to"`
# MAGIC    - `"pf_from"`, `"pf_to"`
# MAGIC    - `"lifecycle_from"`, `"lifecycle_to"`
# MAGIC    - `"ownership_from"`, `"ownership_to"`
# MAGIC 2. Union and deduplicate boundaries and sources into a **boundary catalog**.
# MAGIC 3. For each boundary, keep an `opened_by_sources` array describing which datasets opened a new segment at that timestamp.
# MAGIC
# MAGIC This gives us a full list of **change points** per anchor key, with provenance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Create minimal segments
# MAGIC
# MAGIC For each anchor key:
# MAGIC
# MAGIC 1. Sort the unique boundary timestamps.
# MAGIC 2. Create consecutive segments from them:
# MAGIC    - `segment i = [boundary[i], boundary[i+1])`
# MAGIC 3. These segments are the **minimal time windows** such that:
# MAGIC    - No relationship, mapping, portfolio version, lifecycle, or ownership interval changes *inside* the segment.
# MAGIC    - Any change happens exactly at segment boundaries.
# MAGIC
# MAGIC > **Minimal segments** = the smallest contiguous time slices where every relevant dimension (chain, mapping, portfolio, lifecycle, ownership) is stable.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6: Annotate each segment
# MAGIC
# MAGIC For each minimal segment and anchor key, we determine:
# MAGIC
# MAGIC - Is there an active relationship chain for the full segment? (`has_relpath`)
# MAGIC - Is there an active asset↔portfolio mapping? (`has_asst_invp_map`)
# MAGIC - Is the portfolio version→fund mapping active and matches the same fund? (`has_invp_dim`, `has_invp_bridge`, `has_invp_version`)
# MAGIC - Is there a lifecycle milestone covering the whole segment? (`has_lifecycle_milestone`)
# MAGIC - Is there an ownership milestone covering the whole segment? (`has_ownership_milestone`)
# MAGIC
# MAGIC We then attach:
# MAGIC
# MAGIC - `ownership` – raw path ownership from the chain (0–1)
# MAGIC - `company_chain_ownership_id` – array of pair-level ownership structs
# MAGIC - Lifecycle milestone & phase:
# MAGIC   - `lifecycle_milestone_id`
# MAGIC   - `lifecycle_milestone_type_id`
# MAGIC   - `lifecycle_phase_id`
# MAGIC - Ownership milestone & phase:
# MAGIC   - `ownership_milestone_id`
# MAGIC   - `ownership_milestone_type_id`
# MAGIC   - `ownership_phase_id`
# MAGIC - Provenance:
# MAGIC   - `opened_by_sources` – which boundary types opened the segment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 7: Apply business rules (Lock Box) and flags
# MAGIC
# MAGIC ### Lock Box rule (OWNR_MILE_1002)
# MAGIC
# MAGIC Ownership milestone `OWNR_MILE_1002` represents **Lock Box**.
# MAGIC
# MAGIC 1. We compute the first time each `asset_id` hits the Lock Box milestone:
# MAGIC    - `lock_box_ts = min(ownership.active_from_date where ownership_milestone_id = 'OWNR_MILE_1002')`
# MAGIC 2. For each segment, it is considered **post Lock Box** if either:
# MAGIC    - The segment itself carries the Lock Box milestone, or
# MAGIC    - `lock_box_ts` is not null and `start >= lock_box_ts`.
# MAGIC
# MAGIC We then set:
# MAGIC
# MAGIC - `effective_path_ownership`:
# MAGIC   - `1.0` for all **post-Lock-Box** segments.
# MAGIC   - Raw `ownership` (chain-based) for segments where:
# MAGIC     - `has_relpath`
# MAGIC     - `has_asst_invp_map`
# MAGIC     - `has_invp_dim`
# MAGIC     - `has_invp_bridge`
# MAGIC     - `has_invp_version`
# MAGIC     - `has_lifecycle_milestone`
# MAGIC     - `has_ownership_milestone`
# MAGIC     - and `ownership` is not null
# MAGIC   - `0.0` otherwise (no valid ownership context).
# MAGIC
# MAGIC We also compute:
# MAGIC
# MAGIC - `equal_ownerships` – whether raw `ownership` (coalesced) equals `effective_path_ownership` (coalesced).
# MAGIC
# MAGIC ### Questionable flag
# MAGIC
# MAGIC We define a “good” gate:
# MAGIC
# MAGIC ```text
# MAGIC gates_good =
# MAGIC   has_relpath AND
# MAGIC   has_asst_invp_map AND
# MAGIC   invp_active AND
# MAGIC   has_lifecycle_milestone AND
# MAGIC   has_ownership_milestone
# MAGIC ```
# MAGIC
# MAGIC Then:
# MAGIC
# MAGIC - **Not questionable** (`questionable = false`) if:
# MAGIC   - `gates_good` is true **AND**
# MAGIC   - either:
# MAGIC     - `equal_ownerships` is true, **OR**
# MAGIC     - the segment is **post Lock Box**.
# MAGIC
# MAGIC - **Questionable** (`questionable = true`) otherwise.
# MAGIC
# MAGIC Interpretation:
# MAGIC
# MAGIC - `questionable = false`  
# MAGIC   → Data is complete and internally consistent.  
# MAGIC   The effective ownership either:
# MAGIC   - Matches the raw chain ownership, or
# MAGIC   - Is correctly overridden by the Lock Box rule.
# MAGIC
# MAGIC - `questionable = true`  
# MAGIC   → One or more required conditions is missing or inconsistent, for example:
# MAGIC   - Missing or inactive asset ↔ portfolio mapping
# MAGIC   - Portfolio version does not map back to the same fund
# MAGIC   - Missing lifecycle milestone
# MAGIC   - Missing ownership milestone
# MAGIC   - Broken or inactive ownership chain
# MAGIC   - Raw chain ownership exists but is overridden without a valid Lock Box condition
# MAGIC
# MAGIC This flag is intentionally conservative:  
# MAGIC it is designed to **surface risk and ambiguity**, not to silently “fix” data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 8: Collapse identical adjacent segments
# MAGIC
# MAGIC Working at the **natural grain** (fund, portfolio, version, asset, company chain):
# MAGIC
# MAGIC - Segments are ordered by `start` within each natural-grain partition.
# MAGIC - For each segment, we compare its state to the previous segment using:
# MAGIC   - All non-time state columns (ownership, milestones, flags, etc.)
# MAGIC   - `eqNullSafe` comparisons to avoid false splits due to `NULL`s.
# MAGIC - A new group starts if:
# MAGIC   - It is the first segment, **OR**
# MAGIC   - `end_previous != start_current` (gap or overlap), **OR**
# MAGIC   - Any state column has changed.
# MAGIC
# MAGIC Consecutive segments are **collapsed** when:
# MAGIC
# MAGIC - `end_previous = start_current`, **AND**
# MAGIC - All state columns are identical.
# MAGIC
# MAGIC The result is a compact table where each row represents a **maximal continuous period** of identical state, without redundant time splits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Grain & Composite Key – How rows are uniquely defined
# MAGIC
# MAGIC ### Natural grain (before time)
# MAGIC
# MAGIC ```text
# MAGIC (asset_id,
# MAGIC  fund_core_id,
# MAGIC  investment_portfolio_id,
# MAGIC  invp_version_id,
# MAGIC  full_rel_id,
# MAGIC  company_chain_id)
# MAGIC This represents a unique ownership “context” ignoring time.
# MAGIC ```
# MAGIC
# MAGIC ### Full composite key (including time)
# MAGIC
# MAGIC ```text
# MAGIC (asset_id,
# MAGIC  fund_core_id,
# MAGIC  investment_portfolio_id,
# MAGIC  invp_version_id,
# MAGIC  full_rel_id,
# MAGIC  company_chain_id,
# MAGIC  start,
# MAGIC  end)
# MAGIC ```
# MAGIC
# MAGIC Within this grain:
# MAGIC
# MAGIC - Segments are non-overlapping.
# MAGIC - Time windows are contiguous where state is unchanged.
# MAGIC - Every row is uniquely identifiable and auditable.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Column Definitions
# MAGIC
# MAGIC Please refer to:
# MAGIC
# MAGIC - The column definitions file in the repository, and/or
# MAGIC - The table and column descriptions in the data catalog
# MAGIC
# MAGIC for authoritative business definitions.
# MAGIC
# MAGIC Key columns include:
# MAGIC
# MAGIC - **`ownership`**  
# MAGIC   Raw chain ownership, calculated as the **product of edge-level percentages**.
# MAGIC
# MAGIC - **`company_chain_ownership_id`**  
# MAGIC   Array of structs describing each parent → child edge in the chain and its ownership percentage.
# MAGIC
# MAGIC - **`effective_path_ownership`**  
# MAGIC   Business-rule-adjusted ownership:
# MAGIC   - Raw chain ownership where all validity gates pass
# MAGIC   - Forced to `1.0` for Lock Box and all post–Lock Box segments
# MAGIC
# MAGIC - **`equal_ownerships`**  
# MAGIC   Boolean indicating whether raw ownership equals effective ownership (after NULL-safe coalescing).
# MAGIC
# MAGIC - **`opened_by_sources`**  
# MAGIC   Provenance array describing which datasets opened the segment boundary  
# MAGIC   (relationships, mappings, portfolio versions, lifecycle, ownership).
# MAGIC
# MAGIC - **Validity & state flags**:
# MAGIC   - `has_relpath` – full ownership chain exists and is active
# MAGIC   - `has_asst_invp_map` – asset ↔ portfolio mapping is active
# MAGIC   - `invp_active` – portfolio version ↔ fund mapping is active and consistent
# MAGIC   - `has_lifecycle_milestone` – lifecycle milestone covers the segment
# MAGIC   - `has_ownership_milestone` – ownership milestone covers the segment
# MAGIC   - `questionable` – data quality / consistency risk flag
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Practical Guidance
# MAGIC
# MAGIC ### When to use which table
# MAGIC
# MAGIC - **`historic_ownership`**  
# MAGIC   Use for:
# MAGIC   - Reporting
# MAGIC   - Dashboards
# MAGIC   - Stakeholder-facing analytics  
# MAGIC
# MAGIC   This table prioritises clarity and safety.
# MAGIC
# MAGIC - **`historic_ownership_id`**  
# MAGIC   Use for:
# MAGIC   - Modelling
# MAGIC   - Complex joins
# MAGIC   - Audits
# MAGIC   - Debugging and data quality investigations  
# MAGIC
# MAGIC   This table exposes full identifiers and provenance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common query patterns
# MAGIC
# MAGIC **Ownership “as at” a date (safe view):**
# MAGIC ```sql
# MAGIC WHERE start <= TIMESTAMP '2024-06-30 23:59:59'
# MAGIC   AND end   >  TIMESTAMP '2024-06-30 23:59:59'
# MAGIC   AND questionable = false
# MAGIC ```
# MAGIC
# MAGIC **Ownership over time for a specific asset:**
# MAGIC ```sql
# MAGIC WHERE asset_id = 'ASST_INFR_1234'
# MAGIC   AND questionable = false
# MAGIC ORDER BY start
# MAGIC ```
# MAGIC
# MAGIC **Investigating data-quality issues::**
# MAGIC ```sql
# MAGIC WHERE asset_id = 'ASST_INFR_1234'
# MAGIC   AND questionable = true
# MAGIC ORDER BY start
# MAGIC ```
# MAGIC
# MAGIC These patterns give a reliable, explainable answer to:
# MAGIC
# MAGIC > **Who owns what, when, and under which rules?**
# MAGIC

# COMMAND ----------

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
display(unique_full_rels_raw)

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

display(df)

# COMMAND ----------

distinct_pairs_df = (
    df
    .withColumn("pair", F.explode("parent_child_pairs"))  # flatten to one pair per row
    .select("pair")
    .distinct()
)

distinct_pairs_list_df = distinct_pairs_df.agg(F.collect_set("pair").alias("distinct_parent_child_pairs"))
display(distinct_pairs_list_df)

# COMMAND ----------

# Turn parent-child pairs into rel_id pattern -> "COMP_A.COMP_B"
pairs_rel_ids_df = (
    distinct_pairs_df
    .select(F.concat_ws(".", F.col("pair")).alias("rel_id"))
    .distinct()
)
display(pairs_rel_ids_df)

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
display(result_df)

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
display(result_with_intervals)

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
display(path_pairs_deduped)

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
display(pair_intervals)

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
display(path_pair_intervals)

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
# For each kept (path, segment), we get one row per pair with that pair's percentage on that segment.

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

display(final_df)

# COMMAND ----------

# Next we want to collapse the per-parent-child-pair rows on each valid segment into a single row per (full_rel_id, active_from, active_to) with an array of pairs + percentage. To do this, we need to use the positioning of each pair in the path that we saved in the path_pairs_deduped df.

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

display(final_with_pos)

# COMMAND ----------

# DBTITLE 1,path_pairs should be unique at edge grain
path_pairs_duplicate_check = (
  path_pairs_deduped
    .groupBy("fund_core_id","full_rel_id","company_chain_id","pair_rel_id","pair_pos")
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)
display(path_pairs_duplicate_check)

assert path_pairs_duplicate_check.count() == 0

# COMMAND ----------

# DBTITLE 1,final_with_pos should NOT create extra rows
final_with_pos_duplicate_check = (
  final_with_pos
    .groupBy("fund_core_id","full_rel_id","company_chain_id","pair_rel_id","active_from_date","active_to_date")
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)
display(final_with_pos_duplicate_check)

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
            x.percentage   as percentage,
            x.parent_child_pair as parent_child_pair
          ))
        """).alias("pairs_with_ownership")
    )
)
display(grouped_segments)

# COMMAND ----------

# Multiply edge percentages (0–100 inputs) to a single path percentage per (full_rel_id, segment)

grouped_segments_with_path_pct = (
    grouped_segments
    # Count pairs for basic integrity checks
    .withColumn("num_pairs_in_path", F.size(F.col("pairs_with_ownership")))
    .withColumn("pairs_non_null", F.expr("filter(pairs_with_ownership, x -> x.percentage is not null)"))
    .withColumn("num_pairs_with_pct", F.size(F.col("pairs_non_null")))
    # Compute product of percentages as a fraction:
    # product( (pct_i / 100.0) for i in edges )
    .withColumn(
        "path_fraction",
        F.when(
            F.col("num_pairs_with_pct") == F.col("num_pairs_in_path"),
            F.expr("""
              aggregate(
                transform(pairs_non_null, x -> cast(x.percentage as double)),
                1.0D,
                (acc, y) -> acc * y
              )
            """)
        )
        .otherwise(F.lit(None).cast("double"))  # if any percentage is null, surface as NULL to flag data issue
    )
    .withColumn("path_fraction_rounded", F.round(F.col("path_fraction"), 6))
)
display(grouped_segments_with_path_pct)

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
        array(struct('OWNR_MILE_1003' as milestone_id, earliest_transcation_date as milestone_date)) AS ownership_milestones,
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
        array(struct('First Transaction' as milestone_id, earliest_transcation_date as milestone_date)) AS lifecycle_milestones,
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
SELECT milestone_type_id, viewing_order FROM {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core WHERE END_AT IS NULL
"""
priority_df = spark.sql(milestone_types_raw_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepping

# COMMAND ----------

def apply_priority_and_intervals(raw_df, priority_df, id_col_prefix):
    """
    1. Cross-date priority (Actuals > Forecasts > Baseline).
    2. Groups milestones into Phase-based intervals.
    3. Collects milestones into an array.
    """
    type_id_col = f"{id_col_prefix}_milestone_type_id"
    milestone_id_col = f"{id_col_prefix}_milestone_id"
    phase_id_col = f"{id_col_prefix}_phase_id"
    NOW = F.current_timestamp()

    # 1. Join priority and filter Cross-Date (Actuals suppress others)
    df = raw_df.join(F.broadcast(priority_df), F.col(type_id_col) == F.col("milestone_type_id"), how="inner")
    w_ms = W.partitionBy("asset_id", milestone_id_col)
    df = df.withColumn("_best_type", F.min("viewing_order").over(w_ms)) \
           .filter(F.col("viewing_order") == F.col("_best_type"))

    # 2. Flag Future vs Past for the Exception
    df = df.withColumn("is_future", F.col("event_date") >= NOW)

    # 3. Collapse to Phase Intervals
    # For past/actuals, we group by Phase. For future, we group by Phase + Event Date + Type.
    group_cols = ["asset_id", phase_id_col, "is_future"]
    # Add date/type to grouping only if it's a future record
    df = df.withColumn("_grp_date", F.when(F.col("is_future"), F.col("event_date")).otherwise(F.lit(None)))
    df = df.withColumn("_grp_type", F.when(F.col("is_future"), F.col("milestone_type_id")).otherwise(F.lit(None)))
    
    phase_intervals = (
        df.groupBy("asset_id", phase_id_col, "is_future", "_grp_date", "_grp_type")
        .agg(
            F.min("event_date").alias("phase_start_date"),
            F.first("milestone_type_id").alias(f"{id_col_prefix}_milestone_type_id"),
            F.collect_list(
                F.struct(
                    F.col(milestone_id_col).alias("milestone_id"),
                    F.col("event_date").alias("milestone_date")
                )
            ).alias(f"{id_col_prefix}_milestones")
        )
    )

    # 4. Create the Timeline (active_from / active_to)
    w_ev = W.partitionBy("asset_id").orderBy("phase_start_date")
    
    return (
        phase_intervals
        .withColumn("active_from_date", F.col("phase_start_date").cast("timestamp"))
        .withColumn("active_to_date", F.lead("phase_start_date").over(w_ev).cast("timestamp"))
        .drop("phase_start_date", "is_future", "_grp_date", "_grp_type")
    )

infra_lifecycle_intervals = apply_priority_and_intervals(infra_lifecycle_raw, priority_df, "lifecycle")
infra_lifecycle_intervals = infra_lifecycle_intervals.select(
        "asset_id",
        "lifecycle_phase_id",
        "lifecycle_milestone_type_id",
        "lifecycle_milestones",
        "active_from_date",
        "active_to_date"
    )
infra_ownership_intervals = apply_priority_and_intervals(infra_ownership_raw, priority_df, "ownership")
infra_ownership_intervals = infra_ownership_intervals.select(
        "asset_id",
        "ownership_phase_id",
        "ownership_milestone_type_id",
        "ownership_milestones",
        "active_from_date",
        "active_to_date"
    )

display(infra_lifecycle_intervals)

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
# invp  = invp_raw.withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))
invp_ver_df = invp_ver_df.withColumn("active_to_date", F.coalesce(F.col("active_to_date"), FAR_FUTURE_TS))

asst_infra_lf = infra_lifecycle_intervals.select(
    "asset_id", "lifecycle_milestones", "lifecycle_milestone_type_id", "lifecycle_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_plat_lf = plat_lifecycle_derived.select(
    "asset_id", "lifecycle_milestones", "lifecycle_milestone_type_id", "lifecycle_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_infra_own = infra_ownership_intervals.select(
    "asset_id", "ownership_milestones", "ownership_milestone_type_id", "ownership_phase_id", "active_from_date", "active_to_date"
).withColumn("active_to_date", F.coalesce("active_to_date", FAR_FUTURE_TS))

asst_plat_own = plat_ownership_derived.select(
    "asset_id", "ownership_milestones", "ownership_milestone_type_id", "ownership_phase_id", "active_from_date", "active_to_date"
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

display(universe.where(F.col("ultimate_child_id")=='COMP_1247').where(F.col("asset_id")=='ASST_INFR_1538'))

display(universe.where(F.col("asset_id")=='ASST_INFR_1503'))

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
)

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

display(anchor_base.where(F.col("ultimate_child_id")=='COMP_1247').where(F.col("asset_id")=='ASST_INFR_1538'))

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

# Union them all
boundary_catalog = (
    rel_bounds
    .unionByName(ai_bounds)
    .unionByName(pf_bounds)
    .unionByName(lfe_bounds)
    .unionByName(own_bounds)
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
        F.col(f"l.{lfe_pfx}lifecycle_milestones").alias("lifecycle_milestones"),
        F.col(f"l.{lfe_pfx}lifecycle_milestone_type_id").alias("lifecycle_milestone_type_id"),
        F.col(f"l.{lfe_pfx}lifecycle_phase_id").alias("lifecycle_phase_id"),

        # ownership (IDs + descriptors)
        F.col(f"o.{own_pfx}ownership_milestones").alias("ownership_milestones"),
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
        F.first("lifecycle_milestones",        True).alias("lifecycle_milestones"),
        F.first("lifecycle_milestone_type_id", True).alias("lifecycle_milestone_type_id"),
        F.first("lifecycle_phase_id",          True).alias("lifecycle_phase_id"),

        # ownership
        F.first("ownership_milestones",        True).alias("ownership_milestones"),
        F.first("ownership_milestone_type_id", True).alias("ownership_milestone_type_id"),
        F.first("ownership_phase_id",          True).alias("ownership_phase_id"),
    )
    .withColumn("has_lifecycle_milestone", F.coalesce(F.size("lifecycle_milestones") > 0, F.lit(False)))
    .withColumn("has_ownership_milestone", F.coalesce(F.size("ownership_milestones") > 0, F.lit(False)))
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

# Extract the first time 'OWNR_MILE_1002' appears for any asset from the unified 'own' dataframe
lock_box = (
    own
    .select(
        F.col(f"{own_pfx}asset_id").alias("asset_id"),
        F.explode(f"{own_pfx}ownership_milestones").alias("ms")
    )
    .where(F.col("ms.milestone_id") == F.lit("OWNR_MILE_1002"))
    .groupBy("asset_id")
    .agg(F.min("ms.milestone_date").alias("lock_box_ts"))
)

# Join this back into your segment_open_events
segment_open_events = (
    segment_open_events
    .join(F.broadcast(lock_box), on="asset_id", how="left")
)

is_post_lock_box_date = (
    F.exists(F.col("ownership_milestones"), lambda x: x.milestone_id == F.lit("OWNR_MILE_1002")) |
    (F.col("lock_box_ts").isNotNull() & (F.col("start") >= F.col("lock_box_ts")))
)

# Force effective_path_ownership to 1.0 if post-lockbox, otherwise keep prior rules
segment_open_events = (
    segment_open_events
    .withColumn(
        "effective_path_ownership",
        F.when(is_post_lock_box_date, F.lit(1.0))  # lockbox forces 1.0 (current + future segments)
         .when(
            F.col("has_lifecycle_milestone") &
            F.col("has_ownership_milestone") &
            F.col("has_asst_invp_map") &
            F.col("has_invp_dim") &
            F.col("has_invp_bridge") &
            F.col("has_invp_version") &
            F.col("has_relpath") &
            F.col("ownership").isNotNull(),
            F.col("ownership")
         )
         .otherwise(F.lit(0.0))
    )
    # Compare coalesced values to avoid NULL surprises
    .withColumn(
        "_ownership_coalesced",
        F.coalesce(F.col("ownership"), F.lit(0.0)).cast("double")
    )
    .withColumn(
        "_effective_coalesced",
        F.coalesce(F.col("effective_path_ownership"), F.lit(0.0)).cast("double")
    )
    .withColumn(
        "equal_ownerships",
        F.col("_ownership_coalesced") == F.col("_effective_coalesced")
    )
)

# Define the gate that must be true for a "non-questionable" row (mapping + lifecycle + ownership + relpath)
gates_good = (
    F.col("has_relpath") &
    F.col("has_asst_invp_map") &
    F.col("has_invp_dim") &
    F.col("has_invp_bridge") &
    F.col("has_invp_version") &
    F.col("has_lifecycle_milestone") &
    F.col("has_ownership_milestone")
)

# Set questionable:
# - Not questionable (False) if all gates_good AND (equal_ownerships is True OR is_post_lock_box_date is True)
#   (post-lockbox is already reflected by effective_path_ownership==1.0, but we use the explicit is_post_lock_box_date check
#    to allow cases where equal_ownerships might be False because ownership wasn't updated yet)
# - Otherwise questionable (True)
segment_open_events = (
    segment_open_events
    .withColumn(
        "questionable",
        F.when(
            gates_good & (F.col("equal_ownerships") | is_post_lock_box_date),
            F.lit(False)
        )
        .otherwise(F.lit(True))
    )
    # drop internal helper cols
    .drop("_ownership_coalesced", "_effective_coalesced")
    .select("opened_by_sources", "start", "end", "asset_id", "lifecycle_milestones", "lifecycle_milestone_type_id", "lifecycle_phase_id", "ownership_milestones", "ownership_milestone_type_id", "ownership_phase_id", "investment_portfolio_id","invp_version_id","fund_core_id","full_rel_id","company_chain_id", "ultimate_parent_id","ultimate_child_id", "ownership", "company_chain_ownership_id", "effective_path_ownership", "equal_ownerships", "has_relpath", "has_asst_invp_map", "has_invp_dim", "has_invp_bridge", "has_invp_version", "has_lifecycle_milestone", "has_ownership_milestone", "questionable")
)

# COMMAND ----------

# partition at your natural grain, EXCLUDING time
NATURAL_GRAIN_PARTITION = ["asset_id","fund_core_id","investment_portfolio_id","invp_version_id","full_rel_id","company_chain_id"]
EXPECTED_KEY = NATURAL_GRAIN_PARTITION + ["start","end"]

# state columns = all non-time columns you care about EXCEPT the group keys
STATE_COLS_ALL = [
    "asset_id",
    "lifecycle_milestones","lifecycle_milestone_type_id","lifecycle_phase_id",
    "ownership_milestones","ownership_milestone_type_id","ownership_phase_id",
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

# COMMAND ----------

dups_out = (
  collapsed
    .groupBy(*EXPECTED_KEY)
    .agg(F.count("*").alias("n"))
    .where("n > 1")
)

if dups_out.limit(1).count() > 0:
    display(dups_out)
    raise ValueError("Duplicate rows detected at expected output grain (including full_rel_id).")

else:
    collapsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FULL_TABLE_NAME)

print(f'Written to {FULL_TABLE_NAME}')

# COMMAND ----------

