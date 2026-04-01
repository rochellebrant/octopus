fundoperations_with_no_registered_investoraccount_check = """
SELECT DISTINCT
  CASE 
    WHEN TRIM(Investor_Account) = '' 
         AND Investor IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.bronze_fund)
    THEN Investor
    ELSE Investor_Account
  END AS Investor_Account,
  Investor_Account_IQId
FROM oegen_data_prod_source.src_blackrock_efront_xio.bronze_investoraccount
"""