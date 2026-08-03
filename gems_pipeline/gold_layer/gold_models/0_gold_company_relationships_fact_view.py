gold_company_relationships_fact_view_sql_code = """
WITH base_data AS (
    SELECT * EXCEPT (__START_AT, __END_AT)
    FROM {silver_prefix}company_relationships_recon
    WHERE __END_AT IS NULL
),

gems_waterlines AS (
    SELECT 
        rel_id, 
        MIN(active_from_date) AS gems_start_date
    FROM base_data
    WHERE source = 'GEMs'
    GROUP BY rel_id
),

reconciled_results AS (
    -- Part 1: GEMs (Explicit column list)
    SELECT 
        rel_id,
        company_path_names,
        ultimate_parent_company_id,
        ultimate_parent_company_name,
        ultimate_child_company_id,
        ultimate_child_company_name,
        source,
        percentage_ownership,
        transaction_type,
        active_from_date,
        active_to_date,
        cdm_investment_portfolio_ids,
        cdm_investment_portfolio_names,
        cdm_fund_ids,
        cdm_fund_names,
        refresh_timestamp
    FROM base_data 
    WHERE source = 'GEMs'

    UNION ALL

    -- Part 2: CDM (Exact same column list and order)
    SELECT 
        b.rel_id,
        b.company_path_names,
        b.ultimate_parent_company_id,
        b.ultimate_parent_company_name,
        b.ultimate_child_company_id,
        b.ultimate_child_company_name,
        b.source,
        b.percentage_ownership,
        b.transaction_type,
        b.active_from_date,
        CASE 
            WHEN gw.gems_start_date IS NOT NULL 
                 AND (b.active_to_date IS NULL OR b.active_to_date > gw.gems_start_date)
            THEN gw.gems_start_date
            ELSE b.active_to_date
        END AS active_to_date,
        b.cdm_investment_portfolio_ids,
        b.cdm_investment_portfolio_names,
        b.cdm_fund_ids,
        b.cdm_fund_names,
        b.refresh_timestamp
    FROM base_data b
    LEFT JOIN gems_waterlines gw ON b.rel_id = gw.rel_id
    WHERE b.source = 'CDM'
      AND (
          gw.gems_start_date IS NULL 
          OR b.active_from_date < gw.gems_start_date
      )
)

SELECT DISTINCT * FROM reconciled_results
ORDER BY rel_id, active_from_date
"""