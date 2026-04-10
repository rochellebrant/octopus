silver_investoraccount_sql_code = """
SELECT
  Fund AS ef_fund_long
  , Short_Name AS ef_fund
  , Investor AS investor
  , Investor_Account AS investor_account
  , Currency AS currency

  , Legal_Name AS legal_name
  , Investor_Type AS investor_type
  , Country AS country
  , Tax_Domicile__Country_ AS tax_domicile
  
  , AML_Compliance_Status AS aml_status
  , PEP_Status AS pep_status  
  , Risk_Level AS risk_level

  , Investor_Account_IQId AS investor_account_id
  , Investor_IQId AS investor_id
  , Fund_IQId AS ef_fund_id

  , Created_on AS created_on
  , Modified_by AS modified_by
  , Modified_on AS modified_date
  , datalake_ingestion_timestamp
  , current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}investoraccount
"""