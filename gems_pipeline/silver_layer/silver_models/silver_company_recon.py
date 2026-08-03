silver_recon_company_sql_code = """
WITH cdm_data AS (
    SELECT 
        cdc.company_core_id, 
        cdc.company_registered_name, 
        NULLIF(TRIM(cdc.company_registered_incorporation_number), '') AS company_registered_incorporation_number,
        
        -- 1. Deduplicates, alphabetises, and joins multiple portfolios into one string
        array_join(
            array_sort(collect_set(NULLIF(TRIM(invp.investment_portfolio_name), ''))), 
            ', '
        ) AS cdm_portfolio,
        
        -- 2. Deduplicates, alphabetises, and joins multiple funds into one string
        array_join(
            array_sort(collect_set(NULLIF(TRIM(fdn.fund_display_name), ''))), 
            ', '
        ) AS cdm_fund

    FROM oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_fact_valuation_point vp 
        ON cdc.company_core_id = vp.company_core_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core invp
        ON invp.investment_portfolio_id = vp.investment_portfolio_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc 
        ON fdc.fund_core_id = invp.invp_fund_core_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn
        ON fdn.fund_name_id = fdc.primary_fund_name_id
    WHERE
        cdc.END_AT IS NULL
        AND vp.END_AT IS NULL
        AND CURRENT_DATE() >= vp.active_from_date 
        AND (vp.active_to_date IS NULL OR CURRENT_DATE() <= vp.active_to_date) 
        AND invp.END_AT IS NULL
        AND fdc.END_AT IS NULL
    -- 3. Group by the company details to ensure 1 row per company
    GROUP BY 
        cdc.company_core_id,
        cdc.company_registered_name,
        cdc.company_registered_incorporation_number
),
gems_data AS (
    SELECT DISTINCT 
        gems_company_id, 
        cdm_company_id, 
        gems_company_name,
        NULLIF(TRIM(gems_company_number), '') AS gems_company_number,
        NULLIF(TRIM(gems_fund), '') AS gems_fund,
        NULLIF(TRIM(gems_portfolio), '') AS gems_portfolio
    FROM {silver_prefix}company_dim
)

SELECT 
    -- 1. ID Mapping (Wrapped in COALESCE to prevent DLT SCD2 failures)
    COALESCE(c.company_core_id, g.cdm_company_id, 'MISSING_IN_CDM') AS reconciled_cdm_id,
    COALESCE(g.gems_company_id, 'MISSING_IN_GEMS') AS gems_company_id,
    
    -- 2. High-Level ID Match Status
    CASE 
        WHEN c.company_core_id IS NOT NULL AND g.cdm_company_id IS NOT NULL THEN 'Matched on ID'
        -- If it's in GEMs but the actual CDM table didn't have it:
        WHEN c.company_core_id IS NULL AND g.cdm_company_id IS NOT NULL THEN 'In GEMs with intended CDM ID (Not found in CDM Table)'
        -- If it's in GEMs and has no CDM mapping whatsoever:
        WHEN c.company_core_id IS NULL AND g.cdm_company_id IS NULL THEN 'Only in GEMs (No CDM ID mapped)'
        WHEN g.cdm_company_id IS NULL THEN 'Only in CDM (Missing in GEMs)'
    END AS id_match_status,

    -- 3. Company Name Comparison
    c.company_registered_name AS cdm_name,
    g.gems_company_name AS gems_name,
    CASE 
        WHEN LOWER(TRIM(c.company_registered_name)) = LOWER(TRIM(g.gems_company_name)) THEN TRUE 
        ELSE FALSE 
    END AS is_name_exact_match,
    
    -- 4. Fuzzy Match / Similarity Score (0.0 to 1.0 using Levenshtein distance)
    CASE 
        WHEN c.company_registered_name IS NULL AND g.gems_company_name IS NULL THEN 1.0
        WHEN c.company_registered_name IS NULL OR g.gems_company_name IS NULL THEN 0.0
        WHEN LENGTH(TRIM(c.company_registered_name)) = 0 AND LENGTH(TRIM(g.gems_company_name)) = 0 THEN 1.0
        ELSE 1.0 - (CAST(levenshtein(LOWER(TRIM(c.company_registered_name)), LOWER(TRIM(g.gems_company_name))) AS DOUBLE) / 
             GREATEST(LENGTH(TRIM(c.company_registered_name)), LENGTH(TRIM(g.gems_company_name))))
    END AS name_similarity_score,

    -- 5. Company Number Comparison
    NULLIF(TRIM(c.company_registered_incorporation_number), '') AS cdm_incorporation_number,
    g.gems_company_number,
    CASE 
        WHEN TRIM(c.company_registered_incorporation_number) = TRIM(g.gems_company_number) THEN TRUE 
        ELSE FALSE 
    END AS is_number_exact_match,

    -- 6. Fund Comparison
    c.cdm_fund,
    g.gems_fund,
    CASE 
        WHEN LOWER(TRIM(c.cdm_fund)) = LOWER(TRIM(g.gems_fund)) THEN TRUE 
        ELSE FALSE 
    END AS is_fund_exact_match,

    -- 7. Portfolio Comparison
    c.cdm_portfolio,
    g.gems_portfolio,
    CASE 
        WHEN LOWER(TRIM(c.cdm_portfolio)) = LOWER(TRIM(g.gems_portfolio)) THEN TRUE 
        ELSE FALSE 
    END AS is_portfolio_exact_match,

    current_timestamp() as refresh_timestamp

FROM cdm_data c
FULL OUTER JOIN gems_data g
    ON c.company_core_id = g.cdm_company_id
ORDER BY 
    id_match_status, 
    name_similarity_score ASC,
    cdm_name ASC
"""