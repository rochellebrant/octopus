gold_company_reconciliation_with_cdm_sql_code = """
SELECT 
    id_match_status AS `reconciliation_status`,
    COUNT(*) AS `total_companies`,
    
    -- Calculate percentages for a quick health check
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (), 1) AS `percentage_of_total`,

    -- Name Matching Metrics
    SUM(CASE WHEN is_name_exact_match = TRUE THEN 1 ELSE 0 END) AS `exact_name_matches`,
    SUM(CASE WHEN is_name_exact_match = FALSE AND name_similarity_score >= 0.85 THEN 1 ELSE 0 END) AS `minor_mismatches`,
    SUM(CASE WHEN is_name_exact_match = FALSE AND name_similarity_score < 0.85 THEN 1 ELSE 0 END) AS `major_mismatches`,
    
    -- Company Number Metrics
    SUM(CASE WHEN is_number_exact_match = TRUE THEN 1 ELSE 0 END) AS `exact_number_matches`,
    SUM(CASE WHEN is_number_exact_match = FALSE THEN 1 ELSE 0 END) AS `number_mismatches`,

    current_timestamp() AS refresh_timestamp

FROM {silver_prefix}company_reconciliation_with_cdm
GROUP BY id_match_status
ORDER BY `total_companies` DESC
"""