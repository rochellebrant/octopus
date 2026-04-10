gold_legal_entity_shareholderings_reconciliation_exceptions_sql_code = """
WITH all_entities AS (
    SELECT cdm_child_company_id AS cdm_id, gems_child_company_id AS gems_id, gems_child_company_name AS gems_name, cdm_child_company_name AS cdm_name
    FROM {silver_prefix}shareholdings_reconciliation WHERE cdm_child_company_id IS NOT NULL AND `__END_AT` IS NULL
    UNION 
    SELECT cdm_parent_company_id AS cdm_id, gems_parent_company_id AS gems_id, gems_parent_company_name AS gems_name, cdm_parent_company_name AS cdm_name
    FROM {silver_prefix}shareholdings_reconciliation WHERE cdm_parent_company_id IS NOT NULL AND `__END_AT` IS NULL
),
entity_flags AS (
    SELECT 
        cdm_id,
        MAX(gems_id) AS gems_id,
        MAX(gems_name) AS gems_name,
        MAX(cdm_name) AS cdm_name,
        MAX(CASE WHEN gems_id IS NOT NULL THEN 1 ELSE 0 END) AS is_in_gems,
        MAX(CASE WHEN cdm_name IS NOT NULL THEN 1 ELSE 0 END) AS is_in_cdm
    FROM all_entities
    GROUP BY cdm_id
)

SELECT 
    cdm_id,
    gems_id,
    COALESCE(gems_name, cdm_name) AS entity_name,
    CASE 
        WHEN is_in_gems = 1 AND is_in_cdm = 0 THEN 'Missing in CDM'
        WHEN is_in_gems = 0 AND is_in_cdm = 1 THEN 'Missing in GEMs'
    END AS missing_status,
    current_timestamp() AS refresh_timestamp
FROM entity_flags
WHERE (is_in_gems = 1 AND is_in_cdm = 0) 
   OR (is_in_gems = 0 AND is_in_cdm = 1)
ORDER BY missing_status, entity_name
"""