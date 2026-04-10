rows_with_missing_fields_check = """
    SELECT
        ef_fund_id AS ef_fund_id_check,
        ef_portfolio_id AS ef_portfolio_id_check,
        REPORT_DATE AS REPORT_DATE_check
    FROM {PREFIX}aum_metrics_enriched
    WHERE NOT
        (
            (asset_names IS NULL 
            OR reporting_asset_type IS NULL 
            OR reporting_asset_lifecycle_phase IS NULL 
            OR reporting_asset_ownership_phase IS NULL 
            OR reporting_asset_country IS NULL
            OR reporting_asset_technology IS NULL
            OR inv_pipeline_type IS NULL
            OR invp_investment_strategy IS NULL
            OR fund_structure_type_name IS NULL
            OR asset_details IS NULL
            )
            AND ef_fund_id NOT LIKE 'n/a%' AND ef_portfolio_id NOT LIKE 'n/a%')
"""