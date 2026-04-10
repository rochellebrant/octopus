silver_relationship_map_reconciliation_with_cdm_sql_code = """
WITH
gems_company_parent_relationship_map AS (
    SELECT gem_rels.* FROM {silver_prefix}company_parent_relationship_map gem_rels
),
cdm_company_parent_relationship_map AS (
    SELECT cdm_rels.* FROM oegen_data_prod_prod.core_data_model.bronze_company_parent_relationship_map cdm_rels
    WHERE cdm_rels.END_AT IS NULL
),
unioned_relationships AS (
    SELECT 
        rel_id, 
        (percentage * 100) AS percentage_ownership, 
        transaction_date,
        transaction_type,
        ultimate_child_id,
        ultimate_parent_id,
        'GEMs' AS source 
    FROM gems_company_parent_relationship_map
    
    UNION ALL
    
    SELECT 
        rel_id, 
        (percentage * 100) AS percentage_ownership, 
        transaction_date,
        NULL as transaction_type,
        ultimate_child_id,
        ultimate_parent_id,
        'CDM' AS source 
    FROM cdm_company_parent_relationship_map
),
time_sliced_relationships AS (
    SELECT 
        rel_id,
        ultimate_parent_id,
        ultimate_child_id,
        source,
        percentage_ownership,
        transaction_type, -- [ADDED HERE: Threading it through the CTE]
        CAST(transaction_date AS DATE) AS active_from_date,
        CAST(LEAD(transaction_date) OVER (
            PARTITION BY source, ultimate_parent_id, ultimate_child_id 
            ORDER BY transaction_date
        ) AS DATE) AS active_to_date
    FROM unioned_relationships
),

-- --- 1. PORTFOLIO & FUND ARRAYS ---

company_invp_fund_mappings AS (
    SELECT 
        cfvp.company_core_id,
        COLLECT_SET(invp.investment_portfolio_id) AS investment_portfolio_ids,
        COLLECT_SET(invp.investment_portfolio_name) AS investment_portfolio_names,
        COLLECT_SET(invp.invp_fund_core_id) AS fund_core_ids,
        COLLECT_SET(fdn.fund_display_name) AS fund_display_names
    FROM oegen_data_prod_prod.core_data_model.bronze_company_fact_valuation_point cfvp
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core_af invp 
        ON cfvp.investment_portfolio_id = invp.investment_portfolio_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc 
        ON fdc.fund_core_id = invp.invp_fund_core_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn 
        ON fdn.fund_name_id = fdc.primary_fund_name_id
    WHERE cfvp.END_AT IS NULL 
      AND invp.END_AT IS NULL 
      AND fdn.END_AT IS NULL 
      AND fdc.END_AT IS NULL
    GROUP BY cfvp.company_core_id
),

-- --- 2. ORDERED & DEDUPED COMPANY NAMES ARRAY PER REL_ID ---

unique_rel_ids AS (
    SELECT DISTINCT rel_id FROM unioned_relationships
),
normalized_paths AS (
    -- Safely convert '.' and '_COMP_' into a single ',' delimiter
    -- This turns 'COMP_1004.COMP_1539_COMP_1539.COMP_1577' into 'COMP_1004,COMP_1539,COMP_1539,COMP_1577'
    SELECT 
        rel_id,
        REPLACE(REPLACE(rel_id, '.', ','), '_COMP_', ',COMP_') AS clean_path_string
    FROM unique_rel_ids
),
exploded_path AS (
    -- Now split by ',' and explode into rows with a position index
    SELECT 
        rel_id,
        pos,
        TRIM(node_id) AS node_id
    FROM normalized_paths
    LATERAL VIEW POSEXPLODE(SPLIT(clean_path_string, ',')) AS pos, node_id 
),
path_names AS (
    -- Fetch the company name for each node in the path
    SELECT 
        ep.rel_id,
        ep.pos,
        cdc.company_registered_name
    FROM exploded_path ep
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc 
        ON TRIM(cdc.company_core_id) = TRIM(ep.node_id) 
        AND cdc.END_AT IS NULL
),
ordered_path_arrays AS (
    -- Re-aggregate the names into an array, keeping the parent-to-child order and deduping
    SELECT 
        rel_id,
        ARRAY_DISTINCT(COLLECT_LIST(company_registered_name)) AS company_path_names
    FROM (
        SELECT rel_id, pos, company_registered_name 
        FROM path_names 
        WHERE company_registered_name IS NOT NULL
        ORDER BY rel_id, pos
    )
    GROUP BY rel_id
)

SELECT 
    ts.rel_id,
    COALESCE(opa.company_path_names, ARRAY()) AS company_path_names,
    
    ts.ultimate_parent_id AS ultimate_parent_company_id,
    cdc_parent.company_registered_name AS ultimate_parent_company_name,
    
    ts.ultimate_child_id AS ultimate_child_company_id,
    cdc_child.company_registered_name AS ultimate_child_company_name,
    
    ts.source,
    ts.percentage_ownership,
    ts.transaction_type,
    ts.active_from_date,
    ts.active_to_date,
    
    COALESCE(cim.investment_portfolio_ids, ARRAY()) AS cdm_investment_portfolio_ids,
    COALESCE(cim.investment_portfolio_names, ARRAY()) AS cdm_investment_portfolio_names,
    COALESCE(cim.fund_core_ids, ARRAY()) AS cdm_fund_ids,
    COALESCE(cim.fund_display_names, ARRAY()) AS cdm_fund_names,

    current_timestamp() AS refresh_timestamp

FROM time_sliced_relationships ts

LEFT JOIN company_invp_fund_mappings cim 
    ON TRIM(cim.company_core_id) = TRIM(ts.ultimate_child_id)

LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc_child 
    ON TRIM(cdc_child.company_core_id) = TRIM(ts.ultimate_child_id) 
    AND cdc_child.END_AT IS NULL
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc_parent 
    ON TRIM(cdc_parent.company_core_id) = TRIM(ts.ultimate_parent_id) 
    AND cdc_parent.END_AT IS NULL

LEFT JOIN ordered_path_arrays opa 
    ON ts.rel_id = opa.rel_id

ORDER BY 
    ts.source, 
    ts.ultimate_parent_id, 
    ts.ultimate_child_id, 
    ts.active_from_date
"""