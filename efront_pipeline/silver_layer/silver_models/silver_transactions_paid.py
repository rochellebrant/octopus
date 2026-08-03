silver_transactions_paid_sql_code = """
SELECT
    -- Identifiers & Core Entities
    Transaction_IQId AS transaction_id,
    Company AS company,
    Company_IQId AS company_id,
    Company_Investor AS company_investor,
    Company_Investor_IQId AS company_investor_id,
    Portfolio AS investment_portfolio,

    -- Transaction Details
    Type AS type,
    Description AS description,
    Complete_Name AS complete_name,
    Instrument AS instrument,
    Instrument_currency AS instrument_currency,
    Investment_Details AS investment_details,
    Index AS index,
    Draft AS draft,

    -- Financial Amounts
    Amount_Paid AS amount_paid,
    Amount_transaction_ AS amount_transaction,
    Exclude_transaction_costs AS exclude_transaction_costs,
    AUM_Reporting AS aum_reporting,

    -- Key Dates
    Effective_date AS effective_date,
    Date_Paid AS date_paid,

    -- Audit & Metadata
    Created_on AS created_on,
    Modified_by AS modified_by,
    Modified_on AS modified_on,
    datalake_ingestion_timestamp,
    current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}transactions_paid
"""