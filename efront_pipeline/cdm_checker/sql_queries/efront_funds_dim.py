efront_funds_dim = '''
WITH
  fund as (
  select
    Fund,
    Short_Name,
    Fund_IQId
  from {bronze_prefix}fund
)
SELECT DISTINCT
  f.Short_Name AS EFRONT_FUND_SHORT,
  f.Fund AS EFRONT_FUND_NAME,
  f.Fund_IQId AS EFRONT_FUND_ID,
  m.cdm_fund_id AS MAPPED_CDM_FUND_ID
FROM fund f
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_fund m
  ON f.Fund_IQId = m.source_fund_id
WHERE UPPER(m.source_system_id) = 'SRCE_SYST_1001'
'''