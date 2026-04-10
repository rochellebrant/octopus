silver_fund_sql_code = """
SELECT
  ef_f.Fund AS ef_fund_long
  , ef_f.Short_Name AS ef_fund
  , fdn.fund_display_name AS cdm_fund
  , Number AS fund_number
  , ef_f.Vintage_Year AS vintage_year
  , ef_f.Description AS fund_description

  , ef_f.OEGEN_Fund_Type AS oegen_fund_type
  , ef_f.Legal_Form AS legal_form
  , ef_f.AIF
  , ef_f.Country AS country
  , ef_f.Closing_Date AS closing_date

  , ef_f.Fund_IQId AS ef_fund_id
  , map_f.cdm_fund_id  
  
  , ef_f.Created_on AS created_on
  , ef_f.Modified_by AS modified_by
  , ef_f.Modified_on AS modified_date
  , ef_f.datalake_ingestion_timestamp
  , current_timestamp() AS refresh_timestamp FROM {bronze_prefix}fund ef_f
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_fund map_f ON ef_f.Fund_IQId = map_f.source_fund_id
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc ON fdc.fund_core_id = map_f.cdm_fund_id
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn ON fdc.primary_fund_name_id = fdn.fund_name_id
WHERE map_f.source_system_id = 'SRCE_SYST_1001'
  AND fdc.END_AT IS NULL
  AND fdn.END_AT IS NULL
"""