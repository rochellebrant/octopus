fundoperations_with_no_registered_investoraccount_check = """
SELECT DISTINCT
  CASE 
    WHEN TRIM(investor_account) = '' 
         AND investor IN (SELECT ef_fund_long FROM {PREFIX}fund)
    THEN investor
    ELSE investor_account
  END AS investor_account,
  investor_account_id
FROM {PREFIX}investoraccount
"""