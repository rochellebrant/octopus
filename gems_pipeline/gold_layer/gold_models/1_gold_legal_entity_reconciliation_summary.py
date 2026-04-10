gold_legal_entity_shareholdings_reconciliation_summary_sql_code = """
WITH all_entities AS (
    -- 1. Grab all the Child entities
    SELECT 
        cdm_child_company_id AS cdm_id,
        gems_child_company_id AS gems_id,
        cdm_child_company_name AS cdm_name
    FROM {silver_prefix}shareholdings_reconciliation
    WHERE cdm_child_company_id IS NOT NULL AND `__END_AT` IS NULL

    UNION 

    -- 2. Grab all the Parent entities and stack them underneath
    SELECT 
        cdm_parent_company_id AS cdm_id,
        gems_parent_company_id AS gems_id,
        cdm_parent_company_name AS cdm_name
    FROM {silver_prefix}shareholdings_reconciliation
    WHERE cdm_parent_company_id IS NOT NULL AND `__END_AT` IS NULL
),
entity_flags AS (
    -- 3. Deduplicate by cdm_id and flag if they exist in GEMs or CDM
    SELECT 
        cdm_id,
        MAX(CASE WHEN gems_id IS NOT NULL THEN 1 ELSE 0 END) AS is_in_gems,
        MAX(CASE WHEN cdm_name IS NOT NULL THEN 1 ELSE 0 END) AS is_in_cdm
    FROM all_entities
    GROUP BY cdm_id
)

-- 4. Calculate the final metrics
SELECT 
    COUNT(*) AS total_unique_entities,
    
    -- In Both (Matches)
    SUM(CASE WHEN is_in_gems = 1 AND is_in_cdm = 1 THEN 1 ELSE 0 END) AS matching_entities_count,
    
    -- 1) Proportion of matching entities
    ROUND(
        CAST(SUM(CASE WHEN is_in_gems = 1 AND is_in_cdm = 1 THEN 1 ELSE 0 END) AS DOUBLE) 
        / NULLIF(COUNT(*), 0) * 100, 
    2) AS matching_entities_percentage,

    -- 2) In GEMs but missing in CDM
    SUM(CASE WHEN is_in_gems = 1 AND is_in_cdm = 0 THEN 1 ELSE 0 END) AS gems_only_missing_in_cdm,

    -- 3) In CDM but missing in GEMs
    SUM(CASE WHEN is_in_gems = 0 AND is_in_cdm = 1 THEN 1 ELSE 0 END) AS cdm_only_missing_in_gems,

    current_timestamp() AS refresh_timestamp

FROM entity_flags
"""