efront_funds_aum_fact = '''
WITH
  aum_all AS (
    SELECT FUND_SHORT FROM {bronze_prefix}aum_fund
    UNION ALL
    SELECT FUND_SHORT FROM {bronze_prefix}aum_asset
    )
  
  , fund as (
  select
    Fund,
    Short_Name,
    Fund_IQId
  from {bronze_prefix}fund
)

SELECT DISTINCT
  aum_all.FUND_SHORT AS EFRONT_FUND_SHORT,
  f.fund AS EFRONT_FUND_NAME,
  f.Fund_IQId AS EFRONT_FUND_ID,
  M.cdm_fund_id AS MAPPED_CDM_FUND_ID
FROM aum_all
LEFT JOIN fund f ON aum_all.FUND_SHORT = f.Short_Name
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_fund m
  ON f.Fund_IQId = m.source_fund_id
WHERE UPPER(m.source_system_id) = 'SRCE_SYST_1001'
'''