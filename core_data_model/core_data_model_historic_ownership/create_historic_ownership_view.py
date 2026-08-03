# Databricks notebook source
# dbutils.widgets.text("catalog", "")
# dbutils.widgets.text("database", "")
# dbutils.widgets.text("table", "")

CATALOG = dbutils.widgets.get("catalog").strip().lower()
DATABASE = dbutils.widgets.get("database").strip().lower()
TABLE = dbutils.widgets.get("table").strip().lower()

if not CATALOG or not DATABASE or not TABLE:
    raise ValueError(f"Missing required parameters: CATALOG='{CATALOG}', DATABASE='{DATABASE}', TABLE='{TABLE}'")

FULL_SCHEMA_NAME = f"{CATALOG}.{DATABASE}"
FULL_TABLE_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"
HISTORIC_OWNERSHIP_TABLE_NAME = "historic_ownership"

SILVER_OWNERSHIP_ASSET_TABLE_NAME = "silver_reporting_overview_asset"
GOLD_OWNERSHIP_ASSET_VIEW_NAME = "gold_reporting_overview_asset"

SILVER_OWNERSHIP_PORTFOLIO_TABLE_NAME = "silver_reporting_overview_investment_portfolio"
GOLD_OWNERSHIP_PORTFOLIO_TABLE_NAME = "gold_reporting_overview_investment_portfolio"

print("Writing to:", FULL_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Historic Ownership
print("Writing to:", FULL_TABLE_NAME)
historic_ownership_query = f'''CREATE OR REPLACE TABLE {FULL_SCHEMA_NAME}.{HISTORIC_OWNERSHIP_TABLE_NAME}
USING DELTA
AS
(
  WITH
  assets AS (
      SELECT asset_infra_id AS asset_id, asset_name
      FROM {FULL_SCHEMA_NAME}.bronze_asset_infra_dim_core
      WHERE asset_infra_id IS NOT NULL AND END_AT IS NULL
      UNION
      SELECT asset_plat_id AS asset_id, asset_platform_name AS asset_name
      FROM {FULL_SCHEMA_NAME}.bronze_asset_platform_dim_core
      WHERE asset_plat_id IS NOT NULL AND END_AT IS NULL
  ),
  comp_dim AS (
    SELECT
      company_core_id,
      FIRST(company_registered_name, true) AS company_name
    FROM {FULL_SCHEMA_NAME}.bronze_company_dim_core
    WHERE company_core_id IS NOT NULL AND END_AT IS NULL
    GROUP BY company_core_id
  ),
  comp_map AS (
    SELECT map_from_entries(
          collect_list(named_struct('key', company_core_id, 'value', company_name))
        ) AS comp_map
    FROM comp_dim
  ),
  lm_dim AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', lifecycle_milestone_id, 'value', lifecycle_milestone_name))
    ) AS lm_map
    FROM {FULL_SCHEMA_NAME}.bronze_asset_lifecycle_milestones_dim_core
  ),
  om_dim AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', ownership_milestone_id, 'value', ownership_milestone_name))
    ) AS om_map
    FROM {FULL_SCHEMA_NAME}.bronze_asset_ownership_milestones_dim_core
  ),
  mt_dim AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', milestone_type_id, 'value', milestone_type))
    ) AS mt_map
    FROM {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core
    WHERE END_AT IS NULL
  ),
  
  -- =================================================================================
  -- MAPS FOR METADATA RESOLUTION (BULLETPROOFED)
  -- =================================================================================
  
  -- 1a. Clean and deduplicate technology records using the bronze joins
  tech_base AS (
    SELECT 
      TRIM(UPPER(a.technology_name_id)) AS tech_key,
      FIRST(a.technology_name, true) AS tech_name,
      FIRST(b.technology_core_id, true) AS tech_core_id,
      FIRST(c.technology_grouping_tier_1, true) AS tier_1_id,
      FIRST(e.technology_group_name, true) AS tier_1,
      FIRST(c.technology_grouping_tier_2, true) AS tier_2_id,
      FIRST(f.technology_group_name, true) AS tier_2,
      FIRST(d.technology_category_name, true) AS category
    FROM {FULL_SCHEMA_NAME}.bronze_technology_dim_names a
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_technology_bridge_core_groups b
      ON a.technology_name_id = b.technology_name_id AND b.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_technology_bridge_groups c
      ON c.technology_bridge_groups_id = b.technology_group_id AND c.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_technology_dim_core d
      ON d.technology_core_id = b.technology_core_id AND d.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_technology_dim_groups e
      ON e.technology_groups_id = c.technology_grouping_tier_1 AND e.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_technology_dim_groups f
      ON f.technology_groups_id = c.technology_grouping_tier_2 AND f.END_AT IS NULL
    WHERE a.END_AT IS NULL
    GROUP BY TRIM(UPPER(a.technology_name_id))
  ),
  
  -- 1b. Build the Technology Map
  tech_map AS (
    SELECT map_from_entries(
      collect_list(
        named_struct(
          'key', tech_key, 
          'value', named_struct(
            'name', tech_name,
            'category_id', tech_core_id,
            'category', category,
            'tier_1_id', tier_1_id,
            'tier_1', tier_1,
            'tier_2_id', tier_2_id,
            'tier_2', tier_2
          )
        )
      )
    ) AS tech_lookup
    FROM tech_base
  ),
  
  -- 2. Country Core Map
  country_core_map AS (
    SELECT map_from_entries(
      collect_list(
        named_struct(
          'key', TRIM(UPPER(country_core_id)), 
          'value', named_struct(
            'name', country_name,
            'continent', continent,
            'region', region
          )
        )
      )
    ) AS country_core_lookup
    FROM {FULL_SCHEMA_NAME}.gold_country
  ),
  
  -- 3a. Clean and deduplicate Country Group records
  country_group_base AS (
    SELECT 
      TRIM(UPPER(country_groups_id)) AS group_key,
      FIRST(country_group, true) AS country_group_name
    FROM {FULL_SCHEMA_NAME}.bronze_country_dim_groups
    WHERE country_groups_id IS NOT NULL
    GROUP BY TRIM(UPPER(country_groups_id))
  ),

  -- 3b. Build the Country Group Map
  country_group_map AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', group_key, 'value', country_group_name))
    ) AS country_group_lookup
    FROM country_group_base
  ),
  -- =================================================================================

  base AS (
    SELECT
      fdn.fund_display_name,
      a.asset_name,
      lp.current_lifecycle_phase_name AS lifecycle_phase,
      mt1.milestone_type              AS lifecycle_milestone_type,
      op.ownership_phase_name         AS ownership_phase,
      mt2.milestone_type              AS ownership_milestone_type,
      i.investment_portfolio_name,
      h.*
    FROM {FULL_TABLE_NAME} h
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_fund_dim_core fdc
      ON h.fund_core_id = fdc.fund_core_id AND fdc.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_fund_dim_names fdn
      ON fdc.primary_fund_name_id = fdn.fund_name_id AND fdn.END_AT IS NULL
    LEFT JOIN assets a
      ON a.asset_id = h.asset_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_investment_portfolio_dim_core_af i
      ON h.investment_portfolio_id = i.investment_portfolio_id AND i.END_AT IS NULL
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_lifecycle_phases_dim_core lp
      ON h.lifecycle_phase_id = lp.current_lifecycle_phase_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_ownership_phases_dim_core op
      ON h.ownership_phase_id = op.ownership_phase_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core mt1
      ON h.lifecycle_milestone_type_id = mt1.milestone_type_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core mt2
      ON h.ownership_milestone_type_id = mt2.milestone_type_id
  ),

  enriched AS (
    SELECT
      c.*,
      
      transform(c.technology_name_id, t_id -> element_at(tm.tech_lookup, TRIM(UPPER(t_id)))) AS _tech_structs,
      transform(c.country_core_id, c_id -> element_at(ccm.country_core_lookup, TRIM(UPPER(c_id)))) AS _country_structs,
      
      transform(c.country_groups_id, g_id -> coalesce(element_at(cgm.country_group_lookup, TRIM(UPPER(g_id))), g_id)) AS country_group_name,
      transform(c.company_chain_id, id -> coalesce(element_at(m.comp_map, id), id)) AS company_chain,
      
      transform(c.lifecycle_milestone_details_id, m_det -> 
        named_struct(
            'milestone_id', m_det.milestone_id,
            'milestone_name', coalesce(element_at(lmm.lm_map, m_det.milestone_id), m_det.milestone_id),
            'milestone_type_id', m_det.milestone_type_id,
            'milestone_type', coalesce(element_at(mtm.mt_map, m_det.milestone_type_id), m_det.milestone_type_id),
            'milestone_date', m_det.milestone_date
        )
      ) AS lifecycle_milestone_details,
      
      transform(c.ownership_milestone_details_id, m_det -> 
        named_struct(
            'milestone_id', m_det.milestone_id,
            'milestone_name', coalesce(element_at(omm.om_map, m_det.milestone_id), m_det.milestone_id),
            'milestone_type_id', m_det.milestone_type_id,
            'milestone_type', coalesce(element_at(mtm.mt_map, m_det.milestone_type_id), m_det.milestone_type_id),
            'milestone_date', m_det.milestone_date
        )
      ) AS ownership_milestone_details,

      transform(c.company_chain_ownership_id, x -> 
        named_struct(
            'pair_rel_id',       x.pair_rel_id,
            'ownership',         x.ownership,
            'parent_child_pair', transform(x.parent_child_pair, id -> coalesce(element_at(m.comp_map, id), id))
        )
      ) AS company_chain_ownership

    FROM base c
    CROSS JOIN comp_map m
    CROSS JOIN lm_dim lmm
    CROSS JOIN om_dim omm
    CROSS JOIN mt_dim mtm
    CROSS JOIN tech_map tm
    CROSS JOIN country_core_map ccm
    CROSS JOIN country_group_map cgm
  )
  SELECT
    -- 1. Temporal Bounds
    CAST(start AS TIMESTAMP) AS start,
    CAST(end   AS TIMESTAMP) AS end,

    -- 2. Core Entities
    fund_display_name AS fund,
    investment_portfolio_name AS investment_portfolio,
    asset_name AS asset,
    CASE WHEN asset_id LIKE 'ASST_INFR_%' THEN 'Infrastructure' ELSE 'Platform' END AS asset_type,
    
    -- 3. Descriptive Attributes (Projected efficiently using dot notation!)
    _tech_structs.name AS technology_name,
    _tech_structs.category AS technology_category_name,
    _tech_structs.tier_1 AS technology_grouping_tier_1,
    _tech_structs.tier_2 AS technology_grouping_tier_2,

    _country_structs.name AS country_name,
    _country_structs.continent AS country_continent,
    _country_structs.region AS country_region,

    country_group_name,
    
    -- 4. Lifecycle & Ownership Status
    CASE 
      WHEN asset_id LIKE 'ASST_PLAT_%' THEN 'Business / Developer' 
      ELSE lifecycle_phase 
    END AS lifecycle_phase,
    lifecycle_milestone_type,
    lifecycle_milestone_details,
    
    ownership_phase,
    ownership_milestone_type,
    ownership_milestone_details,

    -- 5. Chains & Hierarchies
    company_chain,
    company_chain_ownership,

    -- 6. Metrics
    ownership,
    effective_path_ownership,
    
    -- 7. Raw IDs
    fund_core_id,
    investment_portfolio_id,
    invp_version_id AS investment_portfolio_version,
    asset_id,
    technology_name_id,
    technology_core_id AS technology_category_name_id,
    technology_grouping_tier_1_id,
    technology_grouping_tier_2_id,
    country_core_id,
    country_groups_id,
    ultimate_parent_id,
    ultimate_child_id,
    company_chain_id,
    company_chain_ownership_id,
    lifecycle_phase_id,
    lifecycle_milestone_type_id,
    ownership_phase_id,
    ownership_milestone_type_id,

    -- 8. Audit & Boolean Flags
    equal_ownerships,
    questionable,
    opened_by_sources,
    has_relpath,
    has_asst_invp_map,
    has_invp_dim,
    has_invp_bridge,
    has_invp_version,
    has_lifecycle_milestone,
    has_ownership_milestone

  FROM enriched
)
'''
spark.sql(historic_ownership_query)
print(f"Saved to: {FULL_SCHEMA_NAME}.{HISTORIC_OWNERSHIP_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Silver Asset
silver_historic_ownership_asset_query = f"""CREATE OR REPLACE TABLE {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_ASSET_TABLE_NAME}
USING DELTA
AS
(
WITH a AS (
    SELECT 
        *,
        CASE 
            WHEN end > CURRENT_TIMESTAMP() THEN NULL 
            ELSE end
        END AS adjusted_end
    FROM 
        {FULL_SCHEMA_NAME}.{HISTORIC_OWNERSHIP_TABLE_NAME}
    WHERE 
        questionable = false -- Complete data only
        AND start <= CURRENT_TIMESTAMP() -- No future data
        AND lifecycle_milestone_type_id IN ('MILE_TYPE_1002') -- Actual milestones only
        AND ownership_milestone_type_id IN ('MILE_TYPE_1002') -- Actual milestones only
        AND ownership_phase_id IN ('OWNR_PHSE_1002', 'OWNR_PHSE_1003', 'OWNR_PHSE_1004', 'OWNR_PHSE_1005') -- Acquired/Acquisition/Divesting/Divested
        AND effective_path_ownership > 0 -- Only assets that we own
)
SELECT
    -- 1. Temporal Context
    DATE(start) AS active_from_date,
    DATE(adjusted_end) AS active_to_date,

    -- 2. Core Hierarchy
    fund,
    investment_portfolio,
    asset,

    -- 3. Key Metrics
    ROUND(effective_path_ownership * 100, 2) AS perc_ownership,

    -- 4. Asset Attributes (Geography & Technology)
    asset_type,

    country_name AS asset_country,
    country_region AS asset_region,
    country_continent AS asset_continent,
    country_group_name AS asset_country_group,
    
    technology_name AS asset_technology_name,
    technology_category_name AS asset_technology_category_name,
    technology_grouping_tier_1 AS asset_technology_grouping_tier_1,
    technology_grouping_tier_2 AS asset_technology_grouping_tier_2,

    -- 5. Status & Phases
    lifecycle_phase AS asset_lifecycle_phase,
    lifecycle_milestone_details AS asset_lifecycle_milestone_details,

    ownership_phase AS asset_ownership_phase,
    ownership_milestone_details AS asset_ownership_milestone_details,

    -- 6. Complex Stuff
    company_chain,
    company_chain_ownership,

    -- 7. Other IDs
    fund_core_id,
    investment_portfolio_id,
    investment_portfolio_version,
    asset_id,

    country_core_id AS asset_country_id,
    country_groups_id AS asset_country_group_id,

    technology_name_id AS asset_technology_name_id,
    technology_category_name_id AS asset_technology_category_name_id,
    technology_grouping_tier_1_id AS asset_technology_grouping_tier_1_id,
    technology_grouping_tier_2_id AS asset_technology_grouping_tier_2_id,

    lifecycle_phase_id AS asset_lifecycle_phase_id,
    lifecycle_milestone_type_id AS asset_lifecycle_milestone_type_id,
    ownership_phase_id AS asset_ownership_phase_id,
    ownership_milestone_type_id AS asset_ownership_milestone_type_id,

    ultimate_parent_id,
    ultimate_child_id,
    company_chain_id,
    company_chain_ownership_id

FROM a
)
"""
spark.sql(silver_historic_ownership_asset_query)
print(f"Saved to: {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_ASSET_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Gold Asset
gold_historic_ownership_asset_query = f'''CREATE OR REPLACE VIEW {FULL_SCHEMA_NAME}.{GOLD_OWNERSHIP_ASSET_VIEW_NAME}
AS
(
SELECT
    active_from_date AS `Active From`,
    active_to_date AS `Active To`,

    fund AS `Fund`,
    investment_portfolio AS `Investment Portfolio`,
    asset AS `Asset`,
    
    perc_ownership AS `Ownership %`,
    asset_type AS `Asset Type`,
    MAX(CASE WHEN asset_id LIKE 'ASST_PLAT_%' THEN 1 ELSE 0 END) 
        OVER (PARTITION BY investment_portfolio) = 1 AS `Is Platform InvP`,
    
    -- Flattening Arrays to Strings for presentation
    array_join(asset_country, '; ') AS `Asset Country`,
    array_join(asset_region, '; ') AS `Asset Region`,
    array_join(asset_continent, '; ') AS `Asset Continent`,
    array_join(asset_country_group, '; ') AS `Asset Country Group`,
    
    array_join(asset_technology_name, '; ') AS `Asset Technology`,
    array_join(asset_technology_category_name, '; ') AS `Asset Technology Category`,
    array_join(asset_technology_grouping_tier_1, '; ') AS `Asset Technology Grouping Tier 1`,
    array_join(asset_technology_grouping_tier_2, '; ') AS `Asset Technology Grouping Tier 2`,

    asset_lifecycle_phase AS `Asset Lifecycle Phase`,
    asset_ownership_phase AS `Asset Ownership Phase`,

    company_chain AS `Company Chain`,
    company_chain_ownership AS `Company Chain Ownership %`,
    transform(asset_lifecycle_milestone_details, m -> 
        named_struct(
            'milestone_name', m.milestone_name,
            'milestone_type', m.milestone_type,
            'milestone_date', m.milestone_date
        )
    ) AS `Asset Lifecycle Milestones Details`,
    transform(asset_ownership_milestone_details, m -> 
        named_struct(
            'milestone_name', m.milestone_name,
            'milestone_type', m.milestone_type,
            'milestone_date', m.milestone_date
        )
    ) AS `Asset Ownership Milestones Details`

FROM {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_ASSET_TABLE_NAME}
)
'''
spark.sql(gold_historic_ownership_asset_query)
print(f"Saved to: {FULL_SCHEMA_NAME}.{GOLD_OWNERSHIP_ASSET_VIEW_NAME}")

# COMMAND ----------

# DBTITLE 1,Silver Investment Portfolio
silver_historic_ownership_portfolio_query = f'''CREATE OR REPLACE TABLE {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_PORTFOLIO_TABLE_NAME}
USING DELTA
AS
(
WITH val_points AS (
    SELECT DISTINCT 
        company_core_id, 
        investment_portfolio_id, 
        CAST(active_from_date AS TIMESTAMP) AS active_from, 
        CASE WHEN CAST(active_to_date AS TIMESTAMP) > current_timestamp() THEN NULL ELSE CAST(active_to_date AS TIMESTAMP) END AS active_to 
    FROM {FULL_SCHEMA_NAME}.bronze_company_fact_valuation_point 
    WHERE END_AT IS NULL 
      AND invp_valuation_point = 1
      AND CAST(active_from_date AS TIMESTAMP) <= current_timestamp()
),
company_names AS (
    SELECT DISTINCT company_core_id, company_registered_name 
    FROM {FULL_SCHEMA_NAME}.bronze_company_dim_core 
    WHERE END_AT IS NULL
),
parsed AS (
    SELECT
        *,
        try_element_at(company_chain_id, -1) AS underlying_spv_id,
        try_element_at(company_chain, -1) AS underlying_spv,
        CAST(active_from_date AS TIMESTAMP) AS start_ts,
        CASE WHEN CAST(active_to_date AS TIMESTAMP) > current_timestamp() THEN NULL ELSE CAST(active_to_date AS TIMESTAMP) END AS end_ts,
        
        -- 1. REPORTING RULE: Active Filter
        CASE WHEN asset_ownership_phase_id IN ('OWNR_PHSE_1003', 'OWNR_PHSE_1002', 'OWNR_PHSE_1004') THEN 1 ELSE 0 END AS is_active_for_reporting

    FROM {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_ASSET_TABLE_NAME}
),
boundaries AS (
    SELECT investment_portfolio_id, start_ts AS ts FROM parsed WHERE start_ts IS NOT NULL
    UNION ALL
    SELECT investment_portfolio_id, COALESCE(end_ts, timestamp'9999-12-31') AS ts FROM parsed
    UNION ALL
    SELECT investment_portfolio_id, active_from AS ts FROM val_points WHERE active_from IS NOT NULL
    UNION ALL
    SELECT investment_portfolio_id, COALESCE(active_to, timestamp'9999-12-31') AS ts FROM val_points
),
periods AS (
    SELECT
        investment_portfolio_id,
        ts AS period_start,
        LEAD(ts) OVER (PARTITION BY investment_portfolio_id ORDER BY ts) AS period_end
    FROM (SELECT DISTINCT investment_portfolio_id, ts FROM boundaries)
),
valid_periods AS (
    SELECT * FROM periods WHERE period_start < period_end
),
expanded AS (
    SELECT
        p.period_start,
        p.period_end,
        p.investment_portfolio_id,
        h.investment_portfolio_version,
        h.investment_portfolio,
        h.fund_core_id, 
        h.fund,
        h.asset_id,
        h.asset,
        h.perc_ownership,
        h.asset_type,
        
        h.asset_lifecycle_phase,
        h.asset_ownership_phase,
        h.asset_country,
        h.asset_continent,
        h.asset_region,
        h.asset_country_group,              
        h.asset_technology_name,
        h.asset_technology_category_name,
        h.asset_technology_grouping_tier_1,
        h.asset_technology_grouping_tier_2,

        h.asset_country_id,
        h.asset_country_group_id,
        h.asset_technology_name_id,
        h.asset_technology_category_name_id,
        h.asset_technology_grouping_tier_1_id,
        h.asset_technology_grouping_tier_2_id,
        h.asset_lifecycle_phase_id,
        h.asset_lifecycle_milestone_type_id,
        h.asset_ownership_phase_id,
        h.asset_ownership_milestone_type_id,

        h.is_active_for_reporting,
        h.underlying_spv_id,
        h.underlying_spv,
        h.company_chain,
        h.company_chain_id,
        h.start_ts,
        v.company_core_id AS val_company_id,
        cn.company_registered_name AS val_company_name
    FROM valid_periods p
    JOIN parsed h
      ON h.investment_portfolio_id = p.investment_portfolio_id
     AND h.start_ts < p.period_end 
     AND COALESCE(h.end_ts, timestamp'9999-12-31') > p.period_start
    LEFT JOIN val_points v
      ON v.investment_portfolio_id = p.investment_portfolio_id
     AND array_contains(h.company_chain_id, v.company_core_id)
     AND v.active_from < p.period_end 
     AND COALESCE(v.active_to, timestamp'9999-12-31') > p.period_start
    LEFT JOIN company_names cn
      ON v.company_core_id = cn.company_core_id
),
expanded_filtered AS (
    SELECT * FROM (
        SELECT 
            *,
            MAX(CASE WHEN asset_type = 'Platform' THEN 1 ELSE 0 END) OVER (PARTITION BY period_start, period_end, investment_portfolio_id) AS has_platform
        FROM expanded
    )
    WHERE has_platform = 0 OR asset_type = 'Platform'
),
aggregated_periods AS (
    SELECT
        period_start,
        period_end,
        investment_portfolio_id,
        FIRST(investment_portfolio) AS investment_portfolio_name,
        FIRST(investment_portfolio_version) AS investment_portfolio_version,
        FIRST(fund_core_id) AS fund_id,
        FIRST(fund) AS fund,
        
        -- ASSET DETAILS STRUCT ARRAY
        array_sort(array_distinct(collect_set(
            named_struct(
                'name', asset,
                'type', asset_type,
                'country', array_join(asset_country, '; '),
                'continent', array_join(asset_continent, '; '),
                'region', array_join(asset_region, '; '),
                'country_group', array_join(asset_country_group, '; '),
                'technology', array_join(asset_technology_name, '; '),
                'technology_category', array_join(asset_technology_category_name, '; '),
                'technology_tier_1', array_join(asset_technology_grouping_tier_1, '; '),
                'technology_tier_2', array_join(asset_technology_grouping_tier_2, '; '),
                'lifecycle_phase', asset_lifecycle_phase,
                'ownership_phase', asset_ownership_phase
            )
        ))) AS asset_details,

        -- RAW SETS: No more split() needed. Collect, flatten, and dedup native arrays
        array_sort(array_distinct(collect_set(asset_id))) AS asset_ids,
        array_sort(array_distinct(collect_set(asset))) AS raw_asset_names,
        array_sort(array_distinct(collect_set(asset_type))) AS raw_asset_types,
        array_sort(array_distinct(collect_set(asset_lifecycle_phase))) AS raw_lifecycle_phases,
        array_sort(array_distinct(collect_set(asset_ownership_phase))) AS raw_ownership_phases,
        
        array_sort(array_distinct(flatten(collect_set(asset_country)))) AS raw_countries,
        array_sort(array_distinct(flatten(collect_set(asset_country_group)))) AS raw_country_groups,
        array_sort(array_distinct(flatten(collect_set(asset_technology_name)))) AS raw_technologies,
        array_sort(array_distinct(flatten(collect_set(asset_technology_category_name)))) AS raw_technology_categories,
        array_sort(array_distinct(flatten(collect_set(asset_technology_grouping_tier_1)))) AS raw_technology_tier_1,
        array_sort(array_distinct(flatten(collect_set(asset_technology_grouping_tier_2)))) AS raw_technology_tier_2,

        -- RAW SETS (IDs)
        array_sort(array_distinct(flatten(collect_set(asset_country_id)))) AS raw_country_ids,
        array_sort(array_distinct(flatten(collect_set(asset_country_group_id)))) AS raw_country_group_ids,
        array_sort(array_distinct(flatten(collect_set(asset_technology_name_id)))) AS raw_technology_name_ids,
        array_sort(array_distinct(flatten(collect_set(asset_technology_category_name_id)))) AS raw_technology_category_name_ids,
        array_sort(array_distinct(flatten(collect_set(asset_technology_grouping_tier_1_id)))) AS raw_technology_tier_1_ids,
        array_sort(array_distinct(flatten(collect_set(asset_technology_grouping_tier_2_id)))) AS raw_technology_tier_2_ids,
        
        array_sort(array_distinct(collect_set(asset_lifecycle_phase_id))) AS raw_lifecycle_phase_ids,
        array_sort(array_distinct(collect_set(asset_lifecycle_milestone_type_id))) AS raw_lifecycle_milestone_type_ids,
        array_sort(array_distinct(collect_set(asset_ownership_phase_id))) AS raw_ownership_phase_ids,
        array_sort(array_distinct(collect_set(asset_ownership_milestone_type_id))) AS raw_ownership_milestone_type_ids,
        
        -- REPORTING SETS (Active Only)
        array_sort(array_distinct(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset END))) AS rep_asset_names,
        array_sort(array_distinct(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_type END))) AS rep_asset_types,
        array_distinct(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_lifecycle_phase_id END)) AS rep_lifecycle_ids,
        array_distinct(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_ownership_phase_id END)) AS rep_ownership_ids,
        
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_country END))) AS rep_countries,
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_country_group END))) AS rep_country_groups,
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_technology_grouping_tier_2_id END))) AS rep_technology_ids,
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_technology_grouping_tier_2 END))) AS rep_technology_names,

        -- REPORTING SETS (Active Only, Raw CDM IDs — for control-table grouping lookups)
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_country_id END))) AS rep_country_ids,
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_country_group_id END))) AS rep_country_group_ids,
        array_distinct(flatten(collect_set(CASE WHEN is_active_for_reporting=1 THEN asset_technology_name_id END))) AS rep_technology_name_ids,

        -- Chain Tracking & Overrides
        COUNT(DISTINCT CASE WHEN perc_ownership > 0 THEN array_join(company_chain_id, ' -> ') END) AS _total_spv_ownership_paths,
        array_sort(array_distinct(collect_set(CASE WHEN perc_ownership > 0 THEN array_join(company_chain, ' -> ') END))) AS company_chains,
        array_sort(array_distinct(collect_set(CASE WHEN perc_ownership > 0 THEN array_join(company_chain_id, ' -> ') END))) AS company_chain_ids,
        array_sort(array_distinct(collect_set(CASE WHEN perc_ownership > 0 THEN underlying_spv_id END))) AS asset_spv_ids,
        array_sort(array_distinct(collect_set(CASE WHEN perc_ownership > 0 THEN underlying_spv END))) AS asset_spvs,
        
        array_sort(collect_set(CASE WHEN perc_ownership > 0 THEN named_struct('SPV', underlying_spv, 'perc_ownership', perc_ownership, 'chain_path', array_join(company_chain, ' -> ')) END)) AS Ownership,
        array_sort(array_distinct(collect_set(CASE WHEN perc_ownership > 0 THEN perc_ownership END))) AS distinct_spv_ownership_percentages,
        (COUNT(DISTINCT CASE WHEN perc_ownership > 0 THEN perc_ownership END) <= 1) AS _has_uniform_spv_ownership,
        CASE WHEN COUNT(DISTINCT CASE WHEN perc_ownership > 0 THEN perc_ownership END) > 1 
             THEN array_sort(collect_set(CASE WHEN perc_ownership > 0 THEN concat(underlying_spv, ' (', cast(perc_ownership AS string), '% via: ', array_join(company_chain, ' -> '), ')') END))
             ELSE NULL END AS _non_uniform_spv_ownership_breakdown,
        array_sort(array_distinct(collect_set(val_company_id))) AS valuation_company_ids,
        array_sort(array_distinct(collect_set(TRIM(val_company_name)))) AS valuation_companies
        
    FROM expanded_filtered
    GROUP BY period_start, period_end, investment_portfolio_id
),
changes AS (
    SELECT
        *,
        CASE WHEN 
            LAG(investment_portfolio_name) OVER w = investment_portfolio_name AND
            LAG(investment_portfolio_version) OVER w = investment_portfolio_version AND
            LAG(fund_id) OVER w = fund_id AND 
            LAG(asset_details) OVER w = asset_details AND
            LAG(rep_asset_names) OVER w = rep_asset_names AND
            LAG(rep_asset_types) OVER w = rep_asset_types AND
            LAG(rep_lifecycle_ids) OVER w = rep_lifecycle_ids AND
            LAG(rep_ownership_ids) OVER w = rep_ownership_ids AND
            LAG(rep_countries) OVER w = rep_countries AND
            LAG(rep_country_groups) OVER w = rep_country_groups AND
            LAG(rep_technology_ids) OVER w = rep_technology_ids AND
            LAG(rep_country_ids) OVER w = rep_country_ids AND
            LAG(rep_country_group_ids) OVER w = rep_country_group_ids AND
            LAG(rep_technology_name_ids) OVER w = rep_technology_name_ids AND
            
            LAG(raw_country_ids) OVER w = raw_country_ids AND
            LAG(raw_country_group_ids) OVER w = raw_country_group_ids AND
            LAG(raw_technology_name_ids) OVER w = raw_technology_name_ids AND
            LAG(raw_technology_category_name_ids) OVER w = raw_technology_category_name_ids AND
            LAG(raw_technology_tier_1_ids) OVER w = raw_technology_tier_1_ids AND
            LAG(raw_technology_tier_2_ids) OVER w = raw_technology_tier_2_ids AND
            LAG(raw_lifecycle_phase_ids) OVER w = raw_lifecycle_phase_ids AND
            LAG(raw_lifecycle_milestone_type_ids) OVER w = raw_lifecycle_milestone_type_ids AND
            LAG(raw_ownership_phase_ids) OVER w = raw_ownership_phase_ids AND
            LAG(raw_ownership_milestone_type_ids) OVER w = raw_ownership_milestone_type_ids AND
            
            LAG(_total_spv_ownership_paths) OVER w = _total_spv_ownership_paths AND
            LAG(company_chains) OVER w = company_chains AND
            LAG(Ownership) OVER w = Ownership AND
            LAG(distinct_spv_ownership_percentages) OVER w = distinct_spv_ownership_percentages AND
            LAG(_has_uniform_spv_ownership) OVER w = _has_uniform_spv_ownership AND
            LAG(_non_uniform_spv_ownership_breakdown) OVER w = _non_uniform_spv_ownership_breakdown AND
            LAG(valuation_company_ids) OVER w = valuation_company_ids
        THEN 0 ELSE 1 END AS is_changed
    FROM aggregated_periods
    WINDOW w AS (PARTITION BY investment_portfolio_id ORDER BY period_start)
),
grouped_changes AS (
    SELECT *, SUM(is_changed) OVER (PARTITION BY investment_portfolio_id ORDER BY period_start) AS grp FROM changes
),
final_intervals AS (
    SELECT
        MIN(period_start) AS start,
        NULLIF(MAX(period_end), timestamp'9999-12-31') AS end,
        investment_portfolio_id,
        FIRST(investment_portfolio_name) AS investment_portfolio_name,
        FIRST(investment_portfolio_version) AS investment_portfolio_version,
        FIRST(fund_id) AS fund_id,
        FIRST(fund) AS fund,
        
        FIRST(asset_details) AS asset_details,

        FIRST(asset_ids) AS asset_ids,
        FIRST(raw_asset_names) AS raw_asset_names,
        FIRST(raw_asset_types) AS raw_asset_types,
        FIRST(raw_lifecycle_phases) AS raw_lifecycle_phases,
        FIRST(raw_ownership_phases) AS raw_ownership_phases,
        
        FIRST(raw_countries) AS raw_countries,
        FIRST(raw_country_groups) AS raw_country_groups,
        FIRST(raw_technologies) AS raw_technologies,
        FIRST(raw_technology_categories) AS raw_technology_categories,
        FIRST(raw_technology_tier_1) AS raw_technology_tier_1,
        FIRST(raw_technology_tier_2) AS raw_technology_tier_2,

        FIRST(raw_country_ids) AS raw_country_ids,
        FIRST(raw_country_group_ids) AS raw_country_group_ids,
        FIRST(raw_technology_name_ids) AS raw_technology_name_ids,
        FIRST(raw_technology_category_name_ids) AS raw_technology_category_name_ids,
        FIRST(raw_technology_tier_1_ids) AS raw_technology_tier_1_ids,
        FIRST(raw_technology_tier_2_ids) AS raw_technology_tier_2_ids,
        FIRST(raw_lifecycle_phase_ids) AS raw_lifecycle_phase_ids,
        FIRST(raw_lifecycle_milestone_type_ids) AS raw_lifecycle_milestone_type_ids,
        FIRST(raw_ownership_phase_ids) AS raw_ownership_phase_ids,
        FIRST(raw_ownership_milestone_type_ids) AS raw_ownership_milestone_type_ids,

        FIRST(rep_asset_names) AS rep_asset_names,
        FIRST(rep_asset_types) AS rep_asset_types,
        FIRST(rep_lifecycle_ids) AS rep_lifecycle_ids,
        FIRST(rep_ownership_ids) AS rep_ownership_ids,
        FIRST(rep_countries) AS rep_countries,
        FIRST(rep_country_groups) AS rep_country_groups,
        FIRST(rep_technology_ids) AS rep_technology_ids,
        FIRST(rep_technology_names) AS rep_technology_names,
        FIRST(rep_country_ids) AS rep_country_ids,
        FIRST(rep_country_group_ids) AS rep_country_group_ids,
        FIRST(rep_technology_name_ids) AS rep_technology_name_ids,

        FIRST(valuation_company_ids) AS valuation_company_ids,
        FIRST(valuation_companies) AS valuation_companies,
        FIRST(asset_spv_ids) AS asset_spv_ids,
        FIRST(asset_spvs) AS asset_spvs,
        FIRST(_total_spv_ownership_paths) AS _total_spv_ownership_paths,
        FIRST(company_chains) AS company_chains,
        FIRST(company_chain_ids) AS company_chain_ids,
        FIRST(_has_uniform_spv_ownership) AS _has_uniform_spv_ownership,
        FIRST(distinct_spv_ownership_percentages) AS distinct_spv_ownership_percentages,
        FIRST(_non_uniform_spv_ownership_breakdown) AS _non_uniform_spv_ownership_breakdown,
        FIRST(Ownership) AS Ownership
    FROM grouped_changes
    GROUP BY investment_portfolio_id, grp
),
resolved_phases AS (
    SELECT 
        f.*,
        
        CASE 
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1003') THEN 'LIFE_PHSE_1003'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1002') THEN 'LIFE_PHSE_1002'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1004') THEN 'LIFE_PHSE_1004'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1006') THEN 'LIFE_PHSE_1006'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1007') THEN 'LIFE_PHSE_1007'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1005') THEN 'LIFE_PHSE_1005'
            WHEN array_contains(f.rep_lifecycle_ids, 'LIFE_PHSE_1008') THEN 'LIFE_PHSE_1008'
            ELSE array_min(f.rep_lifecycle_ids) 
        END AS winning_lifecycle_id,
        
        CASE
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1003') THEN 'OWNR_PHSE_1003'
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1002') THEN 'OWNR_PHSE_1002'
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1001') THEN 'OWNR_PHSE_1001'
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1004') THEN 'OWNR_PHSE_1004'
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1005') THEN 'OWNR_PHSE_1005'
            WHEN array_contains(f.rep_ownership_ids, 'OWNR_PHSE_1006') THEN 'OWNR_PHSE_1006'
            ELSE array_min(f.rep_ownership_ids) 
        END AS winning_ownership_id
        
    FROM final_intervals f
)
SELECT
    date(r.start) AS `active_from_date`,
    date(r.end) AS `active_to_date`,

    r.fund,
    r.investment_portfolio_name as investment_portfolio,
    s.invp_investment_strategy,
    
    -- RAW Arrays - Passed directly as ARRAY<STRING> to table
    r.raw_asset_names AS asset_names,
    r.raw_asset_types AS asset_types,
    r.raw_lifecycle_phases AS asset_lifecycle_phases,
    r.raw_ownership_phases AS asset_ownership_phases,
    r.raw_countries AS asset_countries,
    r.raw_country_groups AS asset_country_groups,
    r.raw_technologies AS asset_technologies,
    r.raw_technology_categories AS asset_technology_category_names,
    r.raw_technology_tier_1 AS asset_technology_grouping_tier_1,
    r.raw_technology_tier_2 AS asset_technology_grouping_tier_2,

    r.asset_details,

    -- REPORTING RULES RESOLUTION
    CASE WHEN size(r.rep_asset_names) > 0 THEN r.rep_asset_names ELSE r.raw_asset_names END AS reporting_asset_names,
    
    CASE WHEN array_contains(r.rep_asset_types, 'Platform') THEN 'Platform'
         ELSE array_join(array_sort(r.rep_asset_types), ', ') END AS reporting_asset_type,
         
    CASE 
        WHEN array_contains(r.rep_asset_types, 'Platform') THEN 'Business / Developer'
        ELSE ld.current_lifecycle_phase_name 
    END AS reporting_asset_lifecycle_phase,

    od.ownership_phase_name AS reporting_asset_ownership_phase,

    -- Chain & Hierarchy Output
    r.valuation_companies,
    r.asset_spvs,
    r._total_spv_ownership_paths,
    r.company_chains,
    r.Ownership AS spv_ownership_breakdown,
    r.distinct_spv_ownership_percentages,
    r._has_uniform_spv_ownership,
    r._non_uniform_spv_ownership_breakdown,

    r.fund_id,
    r.investment_portfolio_id,
    r.investment_portfolio_version,
    
    -- ID Arrays (For Data Engineering)
    r.asset_ids,
    r.valuation_company_ids,
    r.asset_spv_ids,
    r.company_chain_ids,
    
    r.raw_lifecycle_phase_ids AS asset_lifecycle_phase_ids,
    r.raw_lifecycle_milestone_type_ids AS asset_lifecycle_milestone_type_ids,
    r.raw_ownership_phase_ids AS asset_ownership_phase_ids,
    r.raw_ownership_milestone_type_ids AS asset_ownership_milestone_type_ids,
    r.raw_country_ids AS asset_country_ids,
    r.raw_country_group_ids AS asset_country_group_ids,
    r.raw_technology_name_ids AS asset_technology_name_ids,
    r.raw_technology_category_name_ids AS asset_technology_category_name_ids,
    r.raw_technology_tier_1_ids AS asset_technology_grouping_tier_1_ids,
    r.raw_technology_tier_2_ids AS asset_technology_grouping_tier_2_ids
    
FROM resolved_phases r

-- Joining to centralized dimension tables
LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_lifecycle_phases_dim_core ld 
    ON r.winning_lifecycle_id = ld.current_lifecycle_phase_id AND ld.END_AT IS NULL

LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_ownership_phases_dim_core od 
    ON r.winning_ownership_id = od.ownership_phase_id AND od.END_AT IS NULL

LEFT JOIN {FULL_SCHEMA_NAME}.bronze_investment_portfolio_dim_core s 
    ON r.investment_portfolio_id = s.investment_portfolio_id 
    AND r.investment_portfolio_version = s.invp_version_id
    AND s.END_AT IS NULL
)
'''
spark.sql(silver_historic_ownership_portfolio_query)
print(f"Saved to: {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_PORTFOLIO_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Gold Investment Portfolio
gold_historic_ownership_portfolio_query = f'''CREATE OR REPLACE VIEW {FULL_SCHEMA_NAME}.{GOLD_OWNERSHIP_PORTFOLIO_TABLE_NAME} AS (
    SELECT
        -- Temporal & Core Identifiers
        f.active_from_date AS `Active From Date`,
        f.active_to_date AS `Active To Date`,
        f.fund AS `Fund Name`,
        f.investment_portfolio AS `Investment Portfolio Name`,
        f.investment_portfolio_version AS `Investment Portfolio Version`,
        f.invp_investment_strategy AS `Investment Strategy`,

        -- Reporting Focus (What the business usually wants to see first)
        array_join(f.reporting_asset_names, '; ') AS `Reporting Asset Names`,
        f.reporting_asset_type AS `Reporting Asset Type`,
        f.reporting_asset_lifecycle_phase AS `Reporting Asset Lifecycle Phase`,
        f.reporting_asset_ownership_phase AS `Reporting Asset Ownership Phase`,

        -- All Associated Asset Attributes (The raw, unfiltered aggregations)
        array_join(f.asset_names, '; ') AS `All Associated Assets`,
        array_join(f.asset_types, '; ') AS `All Asset Types`,
        array_join(f.asset_lifecycle_phases, '; ') AS `All Asset Lifecycle Phases`,
        array_join(f.asset_ownership_phases, '; ') AS `All Asset Ownership Phases`,
        array_join(f.asset_countries, '; ') AS `All Asset Countries`,
        array_join(f.asset_country_groups, '; ') AS `All Asset Country Groups`,
        array_join(f.asset_technologies, '; ') AS `All Asset Technologies`,
        array_join(f.asset_technology_category_names, '; ') AS `All Asset Technology Categories`,
        array_join(f.asset_technology_grouping_tier_1, '; ') AS `All Asset Technology Tiers 1`,
        array_join(f.asset_technology_grouping_tier_2, '; ') AS `All Asset Technology Tiers 2`,
        f.asset_details AS `Asset Details Summary`,

        -- Corporate Ownership & SPV Breakdown
        array_join(f.valuation_companies, '; ') AS `Valuation Company Names`,
        array_join(f.asset_spvs, '; ') AS `Underlying Asset SPVs`,
        f.company_chains AS `SPV Ownership Paths`,
        f._total_spv_ownership_paths AS `Total SPV Ownership Paths`,
        f.spv_ownership_breakdown AS `SPV Ownership Breakdown`,
        f.distinct_spv_ownership_percentages AS `Distinct SPV Ownership Percentages`,
        f._has_uniform_spv_ownership AS `Has Uniform SPV Ownership`,
        f._non_uniform_spv_ownership_breakdown AS `Non-Uniform SPV Ownership Breakdown`,

        -- System / Internal IDs (Hiding or grouping these at the end for technical users)
        f.fund_id AS `Fund ID`,
        f.investment_portfolio_id AS `Investment Portfolio ID`,
        f.asset_ids AS `Asset IDs`,
        f.company_chain_ids AS `Ownership Path IDs`,
        f.valuation_company_ids AS `Valuation Company IDs`,
        f.asset_spv_ids AS `Asset SPV IDs`
        
    FROM {FULL_SCHEMA_NAME}.{SILVER_OWNERSHIP_PORTFOLIO_TABLE_NAME} f
)
'''
spark.sql(gold_historic_ownership_portfolio_query)
print(f"Saved to: {FULL_SCHEMA_NAME}.{GOLD_OWNERSHIP_PORTFOLIO_TABLE_NAME}")