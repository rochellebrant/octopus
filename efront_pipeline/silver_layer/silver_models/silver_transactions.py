silver_transactions_sql_code = """
SELECT
    -- Identifiers & Core Entities
    Transaction_IQId AS transaction_id,
    Company_IQId AS company_id,
    Company AS company,
    Company_Investor_IQId AS company_investor_id,
    Company_Investor AS company_investor,
    Portfolio AS investment_portfolio,

    -- Transaction Details
    Type AS type,
    OEGEN_type AS oegen_type,
    Description AS description,
    Complete_Name AS complete_name,
    Instrument AS instrument,
    Investment_Details AS investment_details,
    Index AS index,
    Draft AS draft,

    -- Financial Amounts & Currency
    Instrument_currency AS instrument_currency,
    `Exchange_Rate_Instr_->_Investor` AS exchange_rate_instr_investor,
    Amount AS amount,
    `Amount_-_Instrument` AS amount_instrument,
    Valuation AS valuation,
    `Valuation_-_Instrument` AS valuation_instrument,
    Commitment AS commitment,
    `Commitment_-_Instrument` AS commitment_instrument,
    Exclude_transaction_costs AS exclude_transaction_costs,
    AUM_Reporting AS aum_reporting,

    -- Key Dates
    Effective_date AS effective_date,

    -- Audit & Metadata
    Created_on AS created_on,
    Modified_by AS modified_by,
    Modified_on AS modified_on,
    datalake_ingestion_timestamp AS datalake_ingestion_timestamp,
    current_timestamp() AS refresh_timestamp

FROM {bronze_prefix}transactions
"""