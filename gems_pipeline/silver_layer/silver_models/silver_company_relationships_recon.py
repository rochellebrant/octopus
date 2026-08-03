silver_recon_company_relationships_sql_code = """
WITH
gems_company_parent_relationship_map AS (
    SELECT * FROM {silver_prefix}company_relationships_fact
),
cdm_company_parent_relationship_map AS (
    SELECT cdm_rels.*
    FROM oegen_data_prod_prod.core_data_model.bronze_company_parent_relationship_map cdm_rels
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
        transaction_type,
        CAST(transaction_date AS DATE) AS active_from_date,
        CAST(LEAD(transaction_date) OVER (
            PARTITION BY source, ultimate_parent_id, ultimate_child_id 
            ORDER BY transaction_date
        ) AS DATE) AS active_to_date
    FROM unioned_relationships
),

-- --- 1. PORTFOLIO & FUND ARRAYS (TIME-AWARE) ---

time_aware_fund_mappings AS (
    SELECT 
        ts.rel_id,
        ts.source,
        ts.ultimate_parent_id,
        ts.ultimate_child_id,
        ts.active_from_date,
        COLLECT_SET(f.investment_portfolio_id) AS investment_portfolio_ids,
        COLLECT_SET(f.investment_portfolio) AS investment_portfolio_names,
        COLLECT_SET(f.fund_id) AS fund_core_ids,
        COLLECT_SET(f.fund) AS fund_display_names
    FROM time_sliced_relationships ts
    LEFT JOIN oegen_data_prod_prod.core_data_model.silver_company_fund_portfolio_fact f 
        ON TRIM(ts.ultimate_child_id) = TRIM(f.company_id)
        -- Overlap logic: Relationship Start <= Fund End AND Relationship End >= Fund Start
        -- We use '9999-12-31' to safely handle current (NULL) active_to_dates
        AND CAST(f.active_from_date AS DATE) <= COALESCE(ts.active_to_date, CAST('9999-12-31' AS DATE))
        AND COALESCE(CAST(f.active_to_date AS DATE), CAST('9999-12-31' AS DATE)) >= ts.active_from_date
    GROUP BY 
        ts.rel_id,
        ts.source,
        ts.ultimate_parent_id,
        ts.ultimate_child_id,
        ts.active_from_date
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
    ts.ultimate_parent_id AS ultimate_parent_company_id,
    cdc_parent.company_registered_name AS ultimate_parent_company_name,
    
    ts.ultimate_child_id AS ultimate_child_company_id,
    cdc_child.company_registered_name AS ultimate_child_company_name,
    
    ts.source,
    ts.percentage_ownership,
    ts.transaction_type,
    CAST(ts.active_from_date AS DATE) AS active_from_date,
    CAST(ts.active_to_date AS DATE) AS active_to_date,
    
    COALESCE(tafm.investment_portfolio_ids, ARRAY()) AS cdm_investment_portfolio_ids,
    COALESCE(tafm.investment_portfolio_names, ARRAY()) AS cdm_investment_portfolio_names,
    COALESCE(tafm.fund_core_ids, ARRAY()) AS cdm_fund_ids,
    COALESCE(tafm.fund_display_names, ARRAY()) AS cdm_fund_names,

    COALESCE(opa.company_path_names, ARRAY()) AS company_path_names,

    current_timestamp() AS refresh_timestamp

FROM time_sliced_relationships ts

-- Join logic updated to rely on the new time-aware CTE
LEFT JOIN time_aware_fund_mappings tafm 
    ON ts.rel_id = tafm.rel_id 
    AND ts.source = tafm.source
    AND ts.ultimate_parent_id = tafm.ultimate_parent_id
    AND ts.ultimate_child_id = tafm.ultimate_child_id
    -- Use safe null equality operator (<=>) just in case a relationship has no active_from_date
    AND ts.active_from_date <=> tafm.active_from_date 

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