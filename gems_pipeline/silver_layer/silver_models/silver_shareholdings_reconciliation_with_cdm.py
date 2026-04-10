silver_shareholdings_reconciliation_sql_code = """
WITH gems_full_history AS (
    -- 1. Get the full history for GEMs directly from the silver model
    -- replacing NULL end dates with '9999-12-31' for interval math
    SELECT
        company_core_id AS child_internal_id,
        parent_company_id AS parent_internal_id,
        -- ownership_percentage is already scaled to 100 in the silver_shareholdings model
        ownership_percentage AS gems_pct, 
        transaction_type, -- << ADDED HERE
        CAST(active_from_date AS DATE) AS gems_from,
        COALESCE(CAST(active_to_date AS DATE), CAST('9999-12-31' AS DATE)) AS gems_to
    FROM {silver_prefix}shareholdings
    WHERE company_core_id IS NOT NULL 
      AND parent_company_id IS NOT NULL 
      AND ownership_percentage > 0
),
cdm_full_history AS (
    -- 2. Get the full history for CDM
    SELECT 
        company_core_id AS child_internal_id, 
        parent_company_id AS parent_internal_id, 
        ownership_percentage AS cdm_pct, 
        CAST(active_from_date AS DATE) AS cdm_from,
        COALESCE(CAST(active_to_date AS DATE), CAST('9999-12-31' AS DATE)) AS cdm_to
    FROM oegen_data_prod_prod.core_data_model.bronze_company_fact_parents
    WHERE END_AT IS NULL
),
unified_dates AS (
    -- 3. Combine all unique start and end dates from BOTH systems to create our slicing boundaries
    SELECT child_internal_id, parent_internal_id, gems_from AS slice_date FROM gems_full_history
    UNION
    SELECT child_internal_id, parent_internal_id, gems_to FROM gems_full_history
    UNION
    SELECT child_internal_id, parent_internal_id, cdm_from FROM cdm_full_history
    UNION
    SELECT child_internal_id, parent_internal_id, cdm_to FROM cdm_full_history
),
date_intervals AS (
    -- 4. Create the contiguous time chunks based on the unified dates
    SELECT 
        child_internal_id, 
        parent_internal_id, 
        slice_date AS slice_start,
        LEAD(slice_date) OVER (PARTITION BY child_internal_id, parent_internal_id ORDER BY slice_date) AS slice_end
    FROM unified_dates
),
historical_comparison AS (
    -- 5. Evaluate both systems during each specific slice of time
    SELECT
        v.child_internal_id,
        v.parent_internal_id,
        v.slice_start AS active_from_date,
        NULLIF(v.slice_end, CAST('9999-12-31' AS DATE)) AS active_to_date, -- Revert the 9999-12-31 back to NULL for display
        g.gems_pct,
        g.transaction_type AS gems_transaction_type, -- << ADDED HERE
        c.cdm_pct
    FROM date_intervals v
    -- Map GEMs history into the slice
    LEFT JOIN gems_full_history g
        ON TRIM(v.child_internal_id) = TRIM(g.child_internal_id)
        AND TRIM(v.parent_internal_id) = TRIM(g.parent_internal_id)
        AND v.slice_start >= g.gems_from
        AND v.slice_start < g.gems_to
    -- Map CDM history into the slice
    LEFT JOIN cdm_full_history c
        ON TRIM(v.child_internal_id) = TRIM(c.child_internal_id)
        AND TRIM(v.parent_internal_id) = TRIM(c.parent_internal_id)
        AND v.slice_start >= c.cdm_from
        AND v.slice_start < c.cdm_to
    -- Keep only actual valid slices (start < end) and drop slices where neither owned anything
    WHERE v.slice_start < v.slice_end
      AND (g.gems_pct IS NOT NULL OR c.cdm_pct IS NOT NULL)
),
company_invp_fund_mappings AS (
    -- 6. Map to funds and portfolios
    SELECT DISTINCT
        cfvp.company_core_id, cfvp.investment_portfolio_id, invp.investment_portfolio_name,
        invp.invp_fund_core_id AS fund_core_id, fdn.fund_display_name
    FROM oegen_data_prod_prod.core_data_model.bronze_company_fact_valuation_point cfvp
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core_af invp ON cfvp.investment_portfolio_id = invp.investment_portfolio_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc ON fdc.fund_core_id = invp.invp_fund_core_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn ON fdn.fund_name_id = fdc.primary_fund_name_id
    WHERE cfvp.END_AT IS NULL AND active_to_date IS NULL AND invp.END_AT IS NULL AND fdn.END_AT IS NULL AND fdc.END_AT IS NULL
),
gems_entity_lookup AS (
    -- 7. Safely deduplicate the GEMs entities by internal_id so we can pull their IDs/Names without fanning out
    SELECT internal_id, company_id AS gems_company_id, name AS gems_company_name
    FROM (
        SELECT internal_id, company_id, name,
            ROW_NUMBER() OVER(PARTITION BY internal_id ORDER BY company_id DESC) as rn
        FROM {bronze_prefix}api_entity
        WHERE __END_AT IS NULL AND internal_id IS NOT NULL
    ) WHERE rn = 1
)

SELECT

     -- Child Company Details (Internal, GEMs, and CDM)
    hc.child_internal_id AS cdm_child_company_id,
    cdc_child.company_registered_name AS cdm_child_company_name,
    gems_child.gems_company_id AS gems_child_company_id,
    gems_child.gems_company_name AS gems_child_company_name,

    -- Parent Company Details (Internal, GEMs, and CDM)
    hc.parent_internal_id AS cdm_parent_company_id,
    cdc_parent.company_registered_name AS cdm_parent_company_name,
    gems_parent.gems_company_id AS gems_parent_company_id,
    gems_parent.gems_company_name AS gems_parent_company_name,

    hc.active_from_date AS comparison_slice_start,
    hc.active_to_date AS comparison_slice_end,
    
    hc.gems_transaction_type,

    CAST(hc.cdm_pct AS DOUBLE) AS cdm_ownership_percentage,
    CAST(hc.gems_pct AS DOUBLE) AS gems_ownership_percentage,
    CAST(ROUND((hc.gems_pct - hc.cdm_pct), 4) AS DOUBLE) AS percentage_variance,

    CASE
        WHEN hc.cdm_pct IS NULL THEN '1. Missing in CDM for this time period'
        WHEN hc.gems_pct IS NULL THEN '2. Missing in GEMs for this time period'
        WHEN ROUND(hc.gems_pct, 4) = ROUND(hc.cdm_pct, 4) THEN '3. Perfect Match'
        ELSE '4. Percentage Mismatch'
    END AS match_status,

    COALESCE(cim.fund_core_id, 'Unmapped') AS cdm_fund_id,
    COALESCE(cim.fund_display_name, 'Unmapped') AS cdm_fund,
    COALESCE(cim.investment_portfolio_id, 'Unmapped') AS cdm_investment_portfolio_id,
    COALESCE(cim.investment_portfolio_name, 'Unmapped') AS cdm_investment_portfolio_name,

    current_timestamp() AS refresh_timestamp

FROM historical_comparison hc
-- Mappings and CDM Names
LEFT JOIN company_invp_fund_mappings cim ON TRIM(cim.company_core_id) = TRIM(hc.child_internal_id)
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc_child ON TRIM(cdc_child.company_core_id) = TRIM(hc.child_internal_id) AND cdc_child.END_AT IS NULL
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc_parent ON TRIM(cdc_parent.company_core_id) = TRIM(hc.parent_internal_id) AND cdc_parent.END_AT IS NULL
-- GEMs Names and IDs
LEFT JOIN gems_entity_lookup gems_child ON TRIM(gems_child.internal_id) = TRIM(hc.child_internal_id)
LEFT JOIN gems_entity_lookup gems_parent ON TRIM(gems_parent.internal_id) = TRIM(hc.parent_internal_id)

ORDER BY 
    investment_portfolio_name,
    cdm_child_company_name,
    cdm_parent_company_name,
    comparison_slice_start
"""