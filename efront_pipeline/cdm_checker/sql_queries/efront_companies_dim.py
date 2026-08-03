efront_companies_dim = '''
SELECT DISTINCT
  c.Company AS EFRONT_COMPANY_NAME,
  c.Company_IQId AS EFRONT_COMPANY_ID,
  m.cdm_company_id AS MAPPED_CDM_COMPANY_ID,
  c.Company_Type,
  c.Company_Number,
  c.Address,
  c.VAT_Registration_No,
  c.Technology,
  c.Country,
  c.Original_country
FROM {bronze_prefix}company c
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_company m
  ON m.source_company_id = c.Company_IQId
WHERE UPPER(m.source_system_id) = 'SRCE_SYST_1001'
'''