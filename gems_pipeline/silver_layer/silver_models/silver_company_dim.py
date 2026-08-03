silver_dim_company_sql_code = '''
SELECT
    e.internal_id as cdm_company_id,
    e.company_id as gems_company_id,
    e.name AS gems_company_name,
    NULLIF(TRIM(e.company_number), '') AS gems_company_number,
    NULLIF(TRIM(e.entity_type), '') AS gems_entity_type,
    NULLIF(TRIM(e.company_type), '') AS gems_company_type,
    NULLIF(TRIM(e.status), '') AS gems_status,
    e.registration_date as gems_registration_date,
    NULLIF(TRIM(e.jurisdiction), '') AS gems_jurisdiction,
    NULLIF(TRIM(e.fund), '') AS gems_fund,
    NULLIF(TRIM(e.portfolio), '') AS gems_portfolio,
    NULLIF(TRIM(e.technology), '') AS gems_technology,
    datalake_ingestion_timestamp,
    CURRENT_TIMESTAMP() AS refresh_timestamp
FROM {bronze_prefix}api_entity e
WHERE e.__END_AT IS NULL
'''