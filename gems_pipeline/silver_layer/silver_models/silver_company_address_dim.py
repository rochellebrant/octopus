silver_dim_company_address_sql_code = '''
SELECT
    a.company_id as gems_company_id,
    a.company_name as gems_company_name,
    a.address_id as gems_address_id,
    a.address_type as gems_address_type,
    a.full_address as gems_full_address,

    NULLIF(TRIM(a.suite_apt), '') AS gems_suite_apt,
    NULLIF(TRIM(a.address_line_1), '') AS gems_address_line_1,
    NULLIF(TRIM(a.address_line_2), '') AS gems_address_line_2,
    NULLIF(TRIM(a.address_line_3), '') AS gems_address_line_3,
    NULLIF(TRIM(a.city), '') AS gems_city,
    NULLIF(TRIM(a.county), '') AS gems_county,
    NULLIF(TRIM(a.state_province), '') AS gems_state_province,
    NULLIF(TRIM(a.zip_post_code), '') AS gems_zip_post_code,
    NULLIF(TRIM(a.country), '') AS gems_country,

    a.datalake_ingestion_timestamp,
    CURRENT_TIMESTAMP() AS refresh_timestamp
FROM {bronze_prefix}api_company_address a
WHERE a.__END_AT IS NULL
'''