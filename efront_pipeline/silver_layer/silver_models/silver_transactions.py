silver_transactions_sql_code = """
SELECT
  Effective_date AS effective_date
  , Type AS type
  , Amount AS amount
  , Valuation AS valuation
  , Description AS description
  , Draft AS draft

  , Portfolio AS portfolio
  , Company AS company
  , Company_Investor AS company_investor
  , Complete_Name AS complete_name
  , Instrument AS instrument
  
  , Instrument_currency AS instrument_currency
  , `Amount_-_Instrument` AS amount_instrument
  , `Valuation_-_Instrument` AS valuation_instrument
  , `Exchange_Rate_Instr_->_Investor` AS exchange_rate_instr_investor
  , Commitment AS commitment
  , `Commitment_-_Instrument` AS commitment_instrument

  , AUM_Reporting AS aum_reporting
  , Investment_Details AS investment_details
  , Index AS index
  , Exclude_transaction_costs AS exclude_transaction_costs

  , Transaction_IQId AS transaction_id
  , Company_IQId AS company_id
  , Company_Investor_IQId AS company_investor_id
  
  , Created_on AS created_on
  , Modified_by AS modified_by
  , Modified_on AS modified_date
  , datalake_ingestion_timestamp
  , current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}transactions
"""