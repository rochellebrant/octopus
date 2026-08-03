gold_company_dim_view_sql_code = """
WITH entity AS (
    SELECT * EXCEPT (__START_AT, __END_AT)
    FROM {silver_prefix}company_dim
    WHERE `__END_AT` IS NULL
),

address AS (
    SELECT * EXCEPT (__START_AT, __END_AT)
    FROM {silver_prefix}company_address_dim
    WHERE `__END_AT` IS NULL
),

company_address AS (
    SELECT
        e.cdm_company_id,
        e.gems_company_id,
        e.gems_company_name,
        e.gems_company_number,
        e.gems_entity_type,
        e.gems_company_type,
        e.gems_status,
        e.gems_registration_date,
        e.gems_jurisdiction,
        e.gems_fund,
        e.gems_portfolio,
        e.gems_technology,
        a.gems_address_type,
        a.gems_full_address,
        a.gems_city,
        a.gems_county,
        a.gems_state_province,
        a.gems_zip_post_code,
        a.gems_country,
        CURRENT_TIMESTAMP() AS refresh_timestamp
    FROM entity e
    LEFT JOIN address a
        ON e.gems_company_id = a.gems_company_id
)

SELECT
    gems_company_name AS company_name,
    'GEMs' AS source,
    gems_company_id,
    cdm_company_id,
    gems_entity_type,
    gems_company_type,
    gems_status,
    gems_company_number AS company_number,
    gems_registration_date AS registration_date,
    gems_jurisdiction AS jurisdiction,
    gems_address_type AS address_type,
    gems_full_address AS full_address,
    gems_city AS city,
    gems_county AS county,
    gems_state_province AS state_province,
    gems_zip_post_code AS zip_post_code,
    gems_country AS country,
    refresh_timestamp
FROM company_address

UNION ALL

SELECT
    cdc.company_registered_name AS company_name,
    'CDM' AS source,
    NULL AS gems_company_id,
    cdc.company_core_id AS cdm_company_id,
    NULL AS gems_entity_type,
    NULL AS gems_company_type,
    NULL AS gems_status,
    cdc.company_registered_incorporation_number AS company_number,
    TRY_CAST(cdc.incorporation_date AS DATE) AS registration_date,
    NULL AS jurisdiction,
    NULL AS address_type,
    NULL AS full_address,
    NULL AS city,
    NULL AS county,
    NULL AS state_province,
    NULL AS zip_post_code,
    country.country_name AS country,
    NULL AS refresh_timestamp
FROM oegen_data_prod_prod.core_data_model.bronze_company_dim_core cdc
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_country_dim_core country
    ON cdc.country_core_id = country.country_core_id
    AND country.END_AT IS NULL
WHERE cdc.END_AT IS NULL
  AND cdc.company_core_id NOT IN (
      SELECT DISTINCT cdm_company_id
      FROM entity
      WHERE cdm_company_id IS NOT NULL
  )
"""