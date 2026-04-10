silver_gems_cdm_fund_to_assetco_relationship_map_reconciliation_sql_code = """
WITH cdm_funds AS (
    SELECT DISTINCT company_core_id 
    FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_core
    WHERE END_AT IS NULL
),
asset_cos AS (
  SELECT DISTINCT company_core_id FROM oegen_data_prod_prod.core_data_model.bronze_asset_platform_dim_core
  UNION ALL
  SELECT DISTINCT company_core_id FROM oegen_data_prod_prod.core_data_model.bronze_asset_infra_dim_core
),
gems_and_cdm_rels AS (
    SELECT * FROM {silver_prefix}relationship_map_reconciliation_with_cdm
)

SELECT gems_and_cdm_rels.* FROM gems_and_cdm_rels
INNER JOIN cdm_funds ON cdm_funds.company_core_id = gems_and_cdm_rels.ultimate_parent_company_id
INNER JOIN asset_cos ON asset_cos.company_core_id = gems_and_cdm_rels.ultimate_child_company_id
"""