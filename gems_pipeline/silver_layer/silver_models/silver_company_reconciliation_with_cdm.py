silver_gems_cdm_company_reconciliations_sql_code = """
WITH cdm_data AS (
    SELECT DISTINCT 
        company_core_id, 
        company_registered_name, 
        company_registered_incorporation_number 
    FROM oegen_data_prod_prod.core_data_model.bronze_company_dim_core 
    WHERE END_AT IS NULL
),
gems_data AS (
    SELECT DISTINCT 
        company_id AS gems_unique_id, 
        internal_id AS cdm_company_id, 
        name AS company_name, 
        company_number 
    FROM {bronze_prefix}api_entity
)

SELECT 
    -- 1. Identifier Mapping (Wrapped in COALESCE to prevent DLT SCD2 failures)
    COALESCE(c.company_core_id, g.cdm_company_id, 'MISSING_IN_CDM') AS reconciled_cdm_id,
    COALESCE(g.gems_unique_id, 'MISSING_IN_GEMS') AS gems_unique_id,
    
    -- 2. High-Level ID Match Status
    CASE 
        WHEN c.company_core_id IS NOT NULL AND g.cdm_company_id IS NOT NULL THEN 'Matched on ID'
        WHEN c.company_core_id IS NULL THEN 'Only in GEMs (Missing in CDM)'
        WHEN g.cdm_company_id IS NULL THEN 'Only in CDM (Missing in GEMs)'
    END AS id_match_status,

    -- 3. Company Name Comparison
    c.company_registered_name AS cdm_name,
    g.company_name AS gems_name,
    CASE 
        WHEN LOWER(TRIM(c.company_registered_name)) = LOWER(TRIM(g.company_name)) THEN TRUE 
        ELSE FALSE 
    END AS is_name_exact_match,
    
    -- 4. Fuzzy Match / Similarity Score (0.0 to 1.0 using Levenshtein distance)
    CASE 
        WHEN c.company_registered_name IS NULL AND g.company_name IS NULL THEN 1.0
        WHEN c.company_registered_name IS NULL OR g.company_name IS NULL THEN 0.0
        WHEN LENGTH(TRIM(c.company_registered_name)) = 0 AND LENGTH(TRIM(g.company_name)) = 0 THEN 1.0
        ELSE 1.0 - (CAST(levenshtein(LOWER(TRIM(c.company_registered_name)), LOWER(TRIM(g.company_name))) AS DOUBLE) / 
             GREATEST(LENGTH(TRIM(c.company_registered_name)), LENGTH(TRIM(g.company_name))))
    END AS name_similarity_score,

    -- 5. Company Number Comparison
    c.company_registered_incorporation_number AS cdm_incorporation_number,
    g.company_number AS gems_company_number,
    CASE 
        WHEN TRIM(c.company_registered_incorporation_number) = TRIM(g.company_number) THEN TRUE 
        ELSE FALSE 
    END AS is_number_exact_match,

    current_timestamp() as refresh_timestamp

FROM cdm_data c
FULL OUTER JOIN gems_data g
    ON c.company_core_id = g.cdm_company_id
ORDER BY 
    id_match_status, 
    name_similarity_score ASC
"""