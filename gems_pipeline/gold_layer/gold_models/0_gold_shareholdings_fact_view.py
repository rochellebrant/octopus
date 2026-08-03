gold_shareholdings_fact_view_sql_code = """
WITH gems_start_dates AS (
    -- Step 1: Find the absolute earliest date GEMS records this relationship
    SELECT 
        COALESCE(gems_child_company_id, cdm_child_company_id) AS child_id,
        COALESCE(gems_parent_company_id, cdm_parent_company_id) AS parent_id,
        MIN(to_date(comparison_slice_start)) AS earliest_gems_start
    FROM {silver_prefix}shareholdings_recon
    WHERE gems_ownership_percentage IS NOT NULL
      AND __END_AT IS NULL
    GROUP BY 1, 2
),

filtered_timeline AS (
    -- Step 2: Apply core GEMS priority + historical CDM logic
    SELECT 
        recon.* EXCEPT (recon.`__START_AT`, recon.`__END_AT`),
        g.earliest_gems_start
    FROM {silver_prefix}shareholdings_recon recon
    LEFT JOIN gems_start_dates g
        ON COALESCE(recon.gems_child_company_id, recon.cdm_child_company_id) = g.child_id
       AND COALESCE(recon.gems_parent_company_id, recon.cdm_parent_company_id) = g.parent_id
    WHERE
        recon.__END_AT IS NULL AND 
        (recon.gems_ownership_percentage IS NOT NULL
        OR 
        (
            recon.gems_ownership_percentage IS NULL
            AND (
                to_date(recon.comparison_slice_end) <= g.earliest_gems_start 
                OR g.earliest_gems_start IS NULL
            )
        ))
),

timeline_max_bounds AS (
    -- Step 3: Find what the new "latest" end date is for each group after filtering
    SELECT 
        COALESCE(gems_child_company_id, cdm_child_company_id) AS child_id,
        COALESCE(gems_parent_company_id, cdm_parent_company_id) AS parent_id,
        MAX(to_date(comparison_slice_end)) AS max_sliced_end
    FROM filtered_timeline
    GROUP BY 1, 2
)

-- Step 4: Final Selection & Date Adjustment
SELECT 
    f.cdm_child_company_id,
    f.cdm_parent_company_id,
    
    f.gems_child_company_id,
    f.gems_parent_company_id,
    COALESCE(f.gems_child_company_name, f.cdm_child_company_name) AS child_company_name,
    COALESCE(f.gems_parent_company_name, f.cdm_parent_company_name) AS parent_company_name,
    f.comparison_slice_start AS active_from_date,
    
    -- Open-end the timeline back to NULL if it represents the new max boundary
    CASE 
        WHEN to_date(f.comparison_slice_end) = m.max_sliced_end THEN NULL
        ELSE to_date(f.comparison_slice_end)
    END AS active_to_date,
    
    f.gems_transaction_type,
    COALESCE(f.gems_ownership_percentage, f.cdm_ownership_percentage) AS ownership_percentage,
    
    CASE 
        WHEN f.gems_ownership_percentage IS NULL THEN 'CDM'
        ELSE 'GEMs'
    END AS source,
    
    f.cdm_fund_id,
    f.cdm_fund,
    f.cdm_investment_portfolio_id,
    f.cdm_investment_portfolio_name,
    f.refresh_timestamp
FROM filtered_timeline f
LEFT JOIN timeline_max_bounds m
    ON COALESCE(f.gems_child_company_id, f.cdm_child_company_id) = m.child_id
   AND COALESCE(f.gems_parent_company_id, f.cdm_parent_company_id) = m.parent_id
"""