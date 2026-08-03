silver_fact_company_appointments_sql_code = '''
SELECT
    a.company_internal_id as cdm_company_id,
    a.company_id as gems_company_id,
    a.company_name as gems_company_name,
    e.gems_company_number,
    e.gems_entity_type,
    e.gems_company_type,
    e.gems_jurisdiction,

    a.appointment_type as gems_appointment_type,
    a.appointee_internal_id as cdm_appointee_id,
    a.appointee_entity_id as gems_appointee_id,
    p.name as gems_appointee_name,
    p.gender,
    p.nationality,

    -- Safely determine if the appointment is currently active based on dates
    CASE
      WHEN current_date() < TRY_CAST(a.date_appointed AS DATE) THEN false
      WHEN current_date() > TRY_CAST(a.date_resigned AS DATE) THEN false
      ELSE true
    END as is_active,

    TRY_CAST(a.date_appointed AS DATE) as active_from_date,
    TRY_CAST(a.date_resigned AS DATE) as active_to_date,
    
    -- Safely resolve string hyphens and malformed data into actual NULL dates
    TRY_CAST(a.date_resigned AS DATE) AS date_resigned,
    
    CASE
        WHEN TRY_CAST(a.date_resigned AS DATE) IS NULL THEN TRUE
        ELSE FALSE
    END AS is_active_appointment,
    
    -- Evaluate the highest timestamp across all joined tables safely
    GREATEST(
        a.datalake_ingestion_timestamp,
        COALESCE(p.datalake_ingestion_timestamp, a.datalake_ingestion_timestamp),
        COALESCE(e.datalake_ingestion_timestamp, a.datalake_ingestion_timestamp)
    ) AS source_ingestion_timestamp,

    CURRENT_TIMESTAMP() AS refresh_timestamp

FROM {bronze_prefix}api_appointment a

-- Ensure we only join against the active state of the person
LEFT JOIN {bronze_prefix}api_person p 
  ON a.appointee_entity_id = p.entity_id

-- Ensure we only join against the active state of the company
LEFT JOIN {silver_prefix}company_dim e
  ON a.company_id = e.gems_company_id

-- Ensure we are only reading active appointments
WHERE a.__END_AT IS NULL AND p.__END_AT IS NULL
'''