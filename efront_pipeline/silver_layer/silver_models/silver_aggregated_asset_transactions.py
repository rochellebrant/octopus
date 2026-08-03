silver_aggregated_asset_transactions_sql_code = """
WITH

-- ------------------------------------------------------------------------------------------
-- 1. Cashflow Type Lookup
-- Business Logic: Maps the source system transaction types to reporting Cashflow Types.
-- This acts as an inline lookup table as requested.
-- ------------------------------------------------------------------------------------------
cashflow_type_lookup AS (
    SELECT 'EQ - Conversion in' AS type, 'Injections' AS cashflow_type UNION ALL
    SELECT 'EQ - Conversion out', 'Distributions' UNION ALL
    SELECT 'EQ - Dividend (Cash)', 'Distributions' UNION ALL
    SELECT 'EQ - Purchase/Subscription (w/o Commitment)', 'Injections' UNION ALL
    SELECT 'EQ - Sale/Redemption', 'Sales' UNION ALL
    SELECT 'EQ - Subscription (Capital Issue following Commitment)', 'Injections' UNION ALL
    SELECT 'LN - Advance (following Commitment)', 'Injections' UNION ALL
    SELECT 'LN - Advance/Purchase (w/o commitment)', 'Injections' UNION ALL
    SELECT 'LN - Cash Interest', 'Distributions' UNION ALL
    SELECT 'LN - Conversion in', 'Injections' UNION ALL
    SELECT 'LN - Conversion out', 'Injections' UNION ALL
    SELECT 'LN - Repayment (Principal)', 'Distributions'
),

-- ------------------------------------------------------------------------------------------
-- 2. eFront Transactions (Base)
-- Business Logic (Step 3.1): Filters standard eFront transactions to exclude Commitments, 
-- Valuations, Cash Interest, and Capitalised Interest. It also applies business rules:
-- OEGEN Type must be Null, Exclude transaction costs must be Null or False, and AUM Reporting is True.
-- ------------------------------------------------------------------------------------------
efront_tx_filtered AS (
    SELECT 
        company,
        company_investor,
        investment_portfolio,
        instrument,
        type,
        effective_date,
        instrument_currency,
        amount_instrument
    FROM {silver_prefix}transactions
    WHERE 
        -- Exclude specific transaction types
        lower(type) NOT LIKE '%ln - commitment%'
        AND lower(type) NOT LIKE '%eq - commitment%'
        AND lower(type) NOT LIKE '%ln - valuation%' 
        AND lower(type) NOT LIKE '%eq - valuation%' 
        AND lower(type) NOT LIKE '%capitalised interest%'
        AND lower(type) NOT LIKE '%capitalized interest%'
        AND lower(type) NOT LIKE '%cash interest%'
        -- Apply business rule filters
        AND oegen_type IS NULL
        AND (exclude_transaction_costs IS NULL OR exclude_transaction_costs = FALSE OR lower(CAST(exclude_transaction_costs AS VARCHAR(10))) IN ('false', '0'))
        AND (aum_reporting = TRUE OR lower(CAST(aum_reporting AS VARCHAR(10))) IN ('true', '1'))
        AND (draft IS NULL OR draft = FALSE OR lower(CAST(draft AS VARCHAR(10))) IN ('false', '0'))
),

-- ------------------------------------------------------------------------------------------
-- 3. eFront Transactions Paid
-- Business Logic (Step 3.2 & 3.4): Prepares the paid transactions dataset. 
-- Renames 'date_paid' to 'effective_date' and maps 'amount_transaction' to 'amount_instrument'.
-- Applies the identical business filters as the base transactions.
-- ------------------------------------------------------------------------------------------
efront_tx_paid_filtered AS (
    SELECT 
        company,
        company_investor,
        investment_portfolio,
        instrument,
        type,
        date_paid AS effective_date,
        instrument_currency,
        amount_transaction AS amount_instrument
    FROM {silver_prefix}transactions_paid
    WHERE
        (draft IS NULL OR lower(draft) = 'false')
),

-- ------------------------------------------------------------------------------------------
-- 4. Appended Cashflows with Reporting Date and Type
-- Business Logic (Steps 3.3, 3.5, 3.6, 3.7): Unions the base and paid transactions. 
-- Converts the Effective Date to a month-end Reporting Date using LAST_DAY().
-- Joins to the Cashflow Type Lookup to assign 'Injections', 'Distributions', or 'Sales'.
-- ------------------------------------------------------------------------------------------
appended_cashflows AS (
    SELECT 
        t.company,
        t.company_investor,
        t.investment_portfolio,
        t.instrument,
        t.type,
        lkp.cashflow_type,
        LAST_DAY(t.effective_date) AS report_date, -- Converts to end of month
        t.instrument_currency,
        t.amount_instrument
    FROM (
        SELECT * FROM efront_tx_filtered
        UNION ALL
        SELECT * FROM efront_tx_paid_filtered
    ) t
    LEFT JOIN cashflow_type_lookup lkp 
        ON lower(t.type) = lower(lkp.type)
),

-- ------------------------------------------------------------------------------------------
-- 5. Grouped Cashflow Transactions
-- Business Logic (Step 3.8): Group the appended cashflows to remove duplicates and sum the 
-- amount_instrument by dimensions.
-- ------------------------------------------------------------------------------------------
grouped_cashflows AS (
    SELECT 
        company,
        company_investor,
        investment_portfolio,
        instrument,
        type,
        cashflow_type,
        report_date,
        instrument_currency,

        SUM(amount_instrument) AS amount_instrument
    FROM appended_cashflows
    GROUP BY 
        company, company_investor, investment_portfolio, instrument, type, cashflow_type, report_date, instrument_currency
),

-- ------------------------------------------------------------------------------------------
-- 6. EQ Valuations
-- Business Logic (Step 4): Isolates Equity Valuations, applying the same business filters 
-- (OEGEN Type Null, etc.), converting date to EXACT month-end, and grouping.
-- ------------------------------------------------------------------------------------------
eq_valuations AS (
    SELECT 
        investment_portfolio,
        -- Snaps any date in the month to the exact Month-End (e.g., Feb 15 becomes Feb 28)
        LAST_DAY(effective_date) AS report_date,
        instrument_currency,
        SUM(valuation_instrument) AS eq_amount
    FROM {silver_prefix}transactions
    WHERE lower(type) IN ('eq - valuation')
        AND NULLIF(lower(oegen_type), 'null') IS NULL
        AND (NULLIF(lower(exclude_transaction_costs), 'null') IS NULL OR lower(CAST(exclude_transaction_costs AS VARCHAR(10))) = 'false')
        AND lower(CAST(aum_reporting AS VARCHAR(10))) = 'true'
    GROUP BY 
        investment_portfolio, 
        LAST_DAY(effective_date), 
        instrument_currency
),

-- ------------------------------------------------------------------------------------------
-- 7. LN Valuations
-- Business Logic (Step 4): Isolates Loan Valuations, applying standard business filters,
-- converting date to month-end, and grouping by investment_portfolio and period.
-- ------------------------------------------------------------------------------------------
ln_valuations AS (
    SELECT 
        investment_portfolio,
        -- Snaps any date to the Quarter-End
        LAST_DAY(effective_date) AS report_date,
        instrument_currency,
        SUM(valuation_instrument) AS ln_amount
    FROM {silver_prefix}transactions
    WHERE lower(type) IN ('ln - valuation')
        AND NULLIF(lower(oegen_type), 'null') IS NULL
        AND (NULLIF(lower(exclude_transaction_costs), 'null') IS NULL OR lower(CAST(exclude_transaction_costs AS VARCHAR(10))) = 'false')
        AND lower(CAST(aum_reporting AS VARCHAR(10))) = 'true'
    GROUP BY 
        investment_portfolio, 
        LAST_DAY(effective_date),
        instrument_currency
),

-- ------------------------------------------------------------------------------------------
-- 8. Shareholder Loan Transactions (Base)
-- Business Logic: Pulls all SL transactions and flips the sign. 
-- We keep the raw effective_date here so we can accurately sum them up to the EQ date.
-- ------------------------------------------------------------------------------------------
shareholder_loans_base AS (
    SELECT 
        investment_portfolio,
        effective_date,
        instrument_currency, 
        (amount_instrument * -1) AS sl_amount 
    FROM {silver_prefix}transactions
    WHERE lower(type) NOT IN ('ln - valuation', 'ln - cash interest')
        AND LOWER(description) LIKE '%credit facility loan%'
        AND oegen_type IS NULL
),

-- ------------------------------------------------------------------------------------------
-- 9a. Master Portfolio Date Spine (QUARTERLY)
-- This creates a universal calendar of strictly Quarter-End dates for every portfolio
-- ------------------------------------------------------------------------------------------
master_date_spine AS (
    SELECT investment_portfolio, report_date, instrument_currency FROM eq_valuations
    UNION
    SELECT investment_portfolio, report_date, instrument_currency FROM ln_valuations
    UNION
    -- Snap SL transactions to their respective Quarter-End as well
    SELECT 
        investment_portfolio, 
        LAST_DAY(DATE_TRUNC('quarter', effective_date) + INTERVAL '2 months') AS report_date, 
        instrument_currency 
    FROM shareholder_loans_base
),

-- ------------------------------------------------------------------------------------------
-- 9b. Cumulative Shareholder Loan Balance (Quarterly Snapshots)
-- ------------------------------------------------------------------------------------------
shareholder_loan_cumulative AS (
    SELECT 
        spine.investment_portfolio,
        spine.report_date,
        -- Sums all SL transactions that happened on or before the Quarter-End date
        COALESCE(SUM(sl.sl_amount), 0) AS cumulative_sl_amount 
    FROM master_date_spine spine
    LEFT JOIN shareholder_loans_base sl 
        ON spine.investment_portfolio = sl.investment_portfolio
        AND sl.effective_date <= spine.report_date 
    GROUP BY 
        spine.investment_portfolio, 
        spine.report_date
),

-- ------------------------------------------------------------------------------------------
-- 10. Asset NAV Calculation
-- Applies your exact hierarchy rules, but now exclusively on Quarterly dates
-- ------------------------------------------------------------------------------------------
asset_nav_calculated AS (
    SELECT 
        'Multiple/Mixed' AS company,
        'Multiple/Mixed' AS company_investor,
        spine.investment_portfolio,
        'Portfolio NAV' AS instrument,
        'Portfolio Valuation' AS type,
        'Portfolio Valuation' AS cashflow_type,
        spine.report_date,
        spine.instrument_currency,
        
        -- RULES 1-4 COMBINED
        (
            COALESCE(eq.eq_amount, 0) + 
            CASE 
                WHEN ln.ln_amount IS NOT NULL THEN ln.ln_amount 
                ELSE COALESCE(sl.cumulative_sl_amount, 0) 
            END
        ) AS amount_instrument

    FROM master_date_spine spine
    LEFT JOIN eq_valuations eq 
        ON spine.investment_portfolio = eq.investment_portfolio 
        AND spine.report_date = eq.report_date
        AND spine.instrument_currency = eq.instrument_currency 
    LEFT JOIN ln_valuations ln 
        ON spine.investment_portfolio = ln.investment_portfolio 
        AND spine.report_date = ln.report_date
        AND spine.instrument_currency = ln.instrument_currency
    LEFT JOIN shareholder_loan_cumulative sl 
        ON spine.investment_portfolio = sl.investment_portfolio 
        AND spine.report_date = sl.report_date
),

-- ------------------------------------------------------------------------------------------
-- 11. External Asset Debt Valuations
-- Business Logic (Step 5): Isolates LN Valuations specifically categorized as 'Asset Debt' 
-- via the OEGEN Type field.
-- ------------------------------------------------------------------------------------------
external_asset_debt AS (
    SELECT 
        company,
        company_investor,
        investment_portfolio,
        instrument,
        type,
        'External Debt Valuation' AS cashflow_type,
        LAST_DAY(effective_date) AS report_date,
        instrument_currency,
        SUM(valuation_instrument) AS amount_instrument
    FROM {silver_prefix}transactions
    WHERE lower(type) IN ('ln - valuation')
        AND lower(oegen_type) = 'external asset debt' -- Specific filter for External Debt
    GROUP BY 
        company, company_investor, investment_portfolio, instrument, type, LAST_DAY(effective_date), instrument_currency
),
-- ------------------------------------------------------------------------------------------
-- 12. Final Output Construction
-- Business Logic (Step 6): Unions the grouped cashflows, the calculated Asset NAV records, 
-- and the External Asset Debt valuations into the final consolidated table.
-- ------------------------------------------------------------------------------------------
final_unioned AS 
(
    SELECT company, company_investor, investment_portfolio, instrument, type, cashflow_type, report_date, instrument_currency, amount_instrument 
    FROM grouped_cashflows

    UNION ALL

    SELECT company, company_investor, investment_portfolio, instrument, type, cashflow_type, report_date, instrument_currency, amount_instrument 
    FROM asset_nav_calculated

    UNION ALL

    SELECT company, company_investor, investment_portfolio, instrument, type, cashflow_type, report_date, instrument_currency, amount_instrument 
    FROM external_asset_debt
)
SELECT *, current_timestamp() AS refresh_timestamp FROM final_unioned WHERE amount_instrument IS NOT NULL
"""
