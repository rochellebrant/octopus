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
print("Writing to:", FULL_TABLE_NAME)

# COMMAND ----------

historic_ownership_query = f'''CREATE OR REPLACE TABLE {FULL_SCHEMA_NAME}.historic_ownership
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
  -- NEW: Create a map for Lifecycle Milestone Names (ID -> Name)
  lm_dim AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', lifecycle_milestone_id, 'value', lifecycle_milestone_name))
    ) AS lm_map
    FROM {FULL_SCHEMA_NAME}.bronze_asset_lifecycle_milestones_dim_core
  ),
  -- NEW: Create a map for Ownership Milestone Names (ID -> Name)
  om_dim AS (
    SELECT map_from_entries(
      collect_list(named_struct('key', ownership_milestone_id, 'value', ownership_milestone_name))
    ) AS om_map
    FROM {FULL_SCHEMA_NAME}.bronze_asset_ownership_milestones_dim_core
  ),
  base AS (
    SELECT
      fdn.fund_display_name,
      a.asset_name,
      -- Note: Removed single milestone name columns as they are now arrays
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
    
    -- Removed direct joins to milestone dimension tables (lm, om) 
    -- because join keys are now Arrays in 'h'
      
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_lifecycle_phases_dim_core lp
      ON h.lifecycle_phase_id = lp.current_lifecycle_phase_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_ownership_phases_dim_core op
      ON h.ownership_phase_id = op.ownership_phase_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core mt1
      ON h.lifecycle_milestone_type_id = mt1.milestone_type_id
    LEFT JOIN {FULL_SCHEMA_NAME}.bronze_asset_milestone_types_dim_core mt2
      ON h.ownership_milestone_type_id = mt2.milestone_type_id
  )
  SELECT
    c.opened_by_sources,
    CAST(c.start AS TIMESTAMP) AS start,
    CAST(c.end   AS TIMESTAMP) AS end,
    c.asset_name AS asset,
    CASE WHEN c.asset_id LIKE 'ASST_INFR_%' THEN 'Infrastructure' ELSE 'Platform' END AS asset_type,
    
    -- TRANSFORMED ARRAYS: Resolve IDs to Names inside the array of structs
    transform(c.lifecycle_milestones, m -> 
        named_struct(
            'milestone_name', coalesce(element_at(lmm.lm_map, m.milestone_id), m.milestone_id), 
            'milestone_date', m.milestone_date
        )
    ) AS lifecycle_milestones_details,
    
    c.lifecycle_phase,
    c.lifecycle_milestone_type,
    
    transform(c.ownership_milestones, m -> 
        named_struct(
            'milestone_name', coalesce(element_at(omm.om_map, m.milestone_id), m.milestone_id), 
            'milestone_date', m.milestone_date
        )
    ) AS ownership_milestones_details,
    
    c.ownership_phase,
    c.ownership_milestone_type,

    c.investment_portfolio_name AS investment_portfolio,
    c.invp_version_id           AS investment_portfolio_version,
    c.fund_display_name AS fund,
    c.ownership,
    transform(c.company_chain_id, id -> coalesce(element_at(m.comp_map, id), id)) AS company_chain,
    transform(
    c.company_chain_ownership_id,
    x -> named_struct(
        'pair_rel_id',
          concat_ws(
            '.',
            transform(x.parent_child_pair, id -> coalesce(element_at(m.comp_map, id), id))
          ),
        'percentage',
          x.percentage,
        'parent_child_pair',
          transform(x.parent_child_pair, id -> coalesce(element_at(m.comp_map, id), id))
      )
    ) AS company_chain_ownership,

    c.asset_id,
    
    -- RAW ARRAYS (The new source columns)
    c.lifecycle_milestones,
    c.ownership_milestones,
    
    c.lifecycle_phase_id,
    c.lifecycle_milestone_type_id,
    c.ownership_phase_id,
    c.ownership_milestone_type_id,
    c.investment_portfolio_id,
    c.fund_core_id,
    c.ultimate_parent_id,
    c.ultimate_child_id,
    c.company_chain_id,
    c.company_chain_ownership_id,
    c.effective_path_ownership,
    c.equal_ownerships,
    c.has_relpath,
    c.has_asst_invp_map,
    c.has_invp_dim,
    c.has_invp_bridge,
    c.has_invp_version,
    c.has_lifecycle_milestone,
    c.has_ownership_milestone,
    c.questionable
  FROM base c
  CROSS JOIN comp_map m
  CROSS JOIN lm_dim lmm
  CROSS JOIN om_dim omm
  ORDER BY c.asset_name, c.start, c.end, c.investment_portfolio_name, c.fund_display_name
)
'''
spark.sql(historic_ownership_query)

# COMMAND ----------

