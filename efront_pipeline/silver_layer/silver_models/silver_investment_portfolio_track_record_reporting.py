silver_investment_portfolio_reporting_sql_code = """
WITH ranked_portfolio AS (
    SELECT 
        *,
        CASE 
            WHEN CURRENT_DATE() BETWEEN active_from_date AND COALESCE(active_to_date, '9999-12-31') THEN TRUE 
            ELSE FALSE 
        END AS is_active_portfolio,
        ROW_NUMBER() OVER (
            PARTITION BY fund_id, investment_portfolio_id 
            ORDER BY COALESCE(active_to_date, '9999-12-31') DESC, active_from_date DESC
        ) as rn,
        MIN(active_from_date) OVER (
            PARTITION BY fund_id, investment_portfolio_id
        ) AS invp_active_from_date,
        NULLIF(
            MAX(COALESCE(active_to_date, '9999-12-31')) OVER (
                PARTITION BY fund_id, investment_portfolio_id
            ), 
            '9999-12-31'
        ) AS invp_active_to_date
    FROM oegen_data_prod_prod.core_data_model.silver_reporting_overview_investment_portfolio
),
base_data AS (
    SELECT
        a.fund,
        a.investment_portfolio,
        c.inv_pipeline_type,
        c.invp_investment_strategy,
        a.asset_countries,
        a.asset_technologies,
        a.distinct_spv_ownership_percentages,
        a.asset_lifecycle_phases,
        a.reporting_asset_lifecycle_phase,
        a.asset_types,
        a.reporting_asset_type,
        b.lockbox_date,
        a.fund_id,
        a.investment_portfolio_id,
        a.asset_country_ids,
        a.asset_technology_name_ids,
        a.asset_country_group_ids,
        a.is_active_portfolio,
        a.invp_active_from_date,
        a.invp_active_to_date
    FROM ranked_portfolio a
    LEFT JOIN oegen_data_prod_prod.core_data_model_dev.bronze_investment_portfolio_lockbox b
        ON a.investment_portfolio_id = b.investment_portfolio_id
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core c
        ON a.investment_portfolio_id = c.investment_portfolio_id 
        AND c.END_AT IS NULL
    WHERE a.rn = 1 
),

-- 1. Country Grouping CTEs
country_ids_active_names AS (
    SELECT
        b.fund_id, b.investment_portfolio_id,
        array_distinct(collect_list(gc.country_name)) AS active_country_names
    FROM (
        SELECT fund_id, investment_portfolio_id, t.country_core_id
        FROM base_data
        LATERAL VIEW OUTER explode(asset_country_ids) t AS country_core_id
    ) b
    LEFT JOIN oegen_data_prod_prod.core_data_model.gold_country gc
        ON gc.country_core_id = b.country_core_id
    GROUP BY b.fund_id, b.investment_portfolio_id
),
country_ids_grouped AS (
    SELECT
        b.fund_id, b.investment_portfolio_id,
        array_distinct(collect_list(cg.country_grouping_name)) AS grouped_country_names
    FROM (
        SELECT fund_id, investment_portfolio_id, t.country_core_id
        FROM base_data
        LATERAL VIEW OUTER explode(asset_country_ids) t AS country_core_id
    ) b
    LEFT JOIN oegen_data_prod_source.efront_landing._ref_country_groupings cg
        ON cg.country_core_id = b.country_core_id
    GROUP BY b.fund_id, b.investment_portfolio_id
),
country_group_ids_grouped AS (
    SELECT
        b.fund_id, b.investment_portfolio_id,
        array_distinct(collect_list(bcg.country_group)) AS fallback_country_names
    FROM (
        SELECT fund_id, investment_portfolio_id, t.country_groups_id
        FROM base_data
        LATERAL VIEW OUTER explode(asset_country_group_ids) t AS country_groups_id
    ) b
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_country_dim_groups bcg
        ON bcg.country_groups_id = b.country_groups_id
    GROUP BY b.fund_id, b.investment_portfolio_id
),

-- 2. Technology Grouping CTE
technology_ids_grouped AS (
    SELECT
        b.fund_id, b.investment_portfolio_id,
        array_distinct(collect_list(tg.technology_grouping_name)) AS grouped_technology_names
    FROM (
        SELECT fund_id, investment_portfolio_id, t.technology_name_id
        FROM base_data
        LATERAL VIEW OUTER explode(asset_technology_name_ids) t AS technology_name_id
    ) b
    LEFT JOIN oegen_data_prod_source.efront_landing._ref_tech_groupings tg
        ON tg.technology_name_id = b.technology_name_id
    GROUP BY b.fund_id, b.investment_portfolio_id
),

-- 3. Fund Details CTE
fund_dim AS (
    SELECT
        fdc.fund_core_id,
        fdn.fund_legal_name
    FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn
        ON fdc.primary_fund_name_id = fdn.fund_name_id AND fdn.END_AT IS NULL
    WHERE fdc.END_AT IS NULL
),

-- 4. Portfolio-Level Milestone CTEs (Aggregated to Portfolio Grain)
cod_milestones AS (
    SELECT 
        fund_core_id, 
        investment_portfolio_id,
        MIN(DATE(lm.milestone_date)) AS earliest_asset_cod
    FROM (
        SELECT 
            fund_core_id, 
            investment_portfolio_id, 
            explode(lifecycle_milestone_details) AS lm
        FROM oegen_data_prod_prod.core_data_model.historic_ownership
        WHERE lifecycle_milestone_details IS NOT NULL
    ) exploded_lifecycle
    WHERE 
        lm.milestone_id = 'LIFE_MILE_1021'
        AND lm.milestone_type_id = 'MILE_TYPE_1002'
    GROUP BY 
        fund_core_id, investment_portfolio_id
),
ownership_milestones AS (
    SELECT 
        fund_core_id, 
        investment_portfolio_id,
        MIN(DATE(CASE WHEN om.milestone_id = 'OWNR_MILE_1002' THEN om.milestone_date END)) AS earliest_asset_lockbox,
        MIN(DATE(CASE WHEN om.milestone_id = 'OWNR_MILE_1003' THEN om.milestone_date END)) AS earliest_asset_acquisition
    FROM (
        SELECT 
            fund_core_id, 
            investment_portfolio_id, 
            explode(ownership_milestone_details) AS om
        FROM oegen_data_prod_prod.core_data_model.historic_ownership
        WHERE ownership_milestone_details IS NOT NULL
    ) exploded_ownership
    WHERE 
        om.milestone_type_id = 'MILE_TYPE_1002'
        AND om.milestone_id IN ('OWNR_MILE_1002', 'OWNR_MILE_1003')
    GROUP BY 
        fund_core_id, investment_portfolio_id
)

-- 5. Final Select
SELECT
    bd.fund,
    fd.fund_legal_name,
    bd.investment_portfolio,
    bd.is_active_portfolio,
    bd.inv_pipeline_type,
    bd.invp_investment_strategy,
    
    CASE
        WHEN bd.investment_portfolio_id = 'INVP_1132'
         AND array_contains(coalesce(cian.active_country_names, array()), 'Portugal')
         AND array_contains(coalesce(cian.active_country_names, array()), 'Spain')
        THEN 'Spain'
        WHEN size(coalesce(cig.grouped_country_names, array())) > 1 THEN 'Multi-Country'
        WHEN size(coalesce(cig.grouped_country_names, array())) = 1 THEN element_at(cig.grouped_country_names, 1)
        WHEN size(coalesce(cgg.fallback_country_names, array())) > 1 THEN 'Multi-Country'
        WHEN size(coalesce(cgg.fallback_country_names, array())) = 1 THEN element_at(cgg.fallback_country_names, 1)
        ELSE NULL
    END AS reporting_asset_country,
    
    CASE
        WHEN size(coalesce(tig.grouped_technology_names, array())) = 2
             AND array_contains(tig.grouped_technology_names, 'Battery storage')
        THEN concat(element_at(array_except(tig.grouped_technology_names, array('Battery storage')), 1), ' & Battery storage')
        WHEN size(coalesce(tig.grouped_technology_names, array())) >= 2 THEN 'Multi-Tech Renewables'
        WHEN size(coalesce(tig.grouped_technology_names, array())) = 1 THEN element_at(tig.grouped_technology_names, 1)
        ELSE NULL
    END AS reporting_asset_technology,
    
    CASE 
        WHEN ARRAY_SIZE(bd.distinct_spv_ownership_percentages) = 1 
        THEN ROUND(bd.distinct_spv_ownership_percentages[0], 2)
        ELSE NULL 
    END AS latest_ownership_percentage,
    
    bd.reporting_asset_lifecycle_phase,
    bd.reporting_asset_type,
    bd.lockbox_date,
    
    DATE(bd.invp_active_from_date) AS invp_active_from_date,
    DATE(bd.invp_active_to_date) AS invp_active_to_date,
    
    cm.earliest_asset_cod,
    om.earliest_asset_lockbox,
    om.earliest_asset_acquisition,

    -- Added IDs Below
    bd.fund_id,
    bd.investment_portfolio_id,
    bd.asset_country_ids,
    bd.asset_technology_name_ids,
    bd.asset_country_group_ids,

    current_timestamp() as refresh_timestamp

FROM base_data bd
LEFT JOIN country_ids_active_names cian
    ON bd.fund_id = cian.fund_id AND bd.investment_portfolio_id = cian.investment_portfolio_id
LEFT JOIN country_ids_grouped cig
    ON bd.fund_id = cig.fund_id AND bd.investment_portfolio_id = cig.investment_portfolio_id
LEFT JOIN country_group_ids_grouped cgg
    ON bd.fund_id = cgg.fund_id AND bd.investment_portfolio_id = cgg.investment_portfolio_id
LEFT JOIN technology_ids_grouped tig
    ON bd.fund_id = tig.fund_id AND bd.investment_portfolio_id = tig.investment_portfolio_id
LEFT JOIN fund_dim fd
    ON bd.fund_id = fd.fund_core_id
LEFT JOIN cod_milestones cm
    ON bd.fund_id = cm.fund_core_id 
    AND bd.investment_portfolio_id = cm.investment_portfolio_id
LEFT JOIN ownership_milestones om
    ON bd.fund_id = om.fund_core_id 
    AND bd.investment_portfolio_id = om.investment_portfolio_id
ORDER BY bd.investment_portfolio
"""
