gold_aum_bridge_view_sql_code = """
WITH raw AS (
    SELECT 
        ef_fund_id, ef_portfolio_id, ef_fund, ef_portfolio, REPORT_DATE, AUM,
        coalesce(ASSET_DEBT_CMT, 0) + coalesce(ASSET_EQUITY_CMT, 0) + coalesce(ADJ_UNDRAWNCOMMITMENT, 0) as ASSET_CMTD_EQUTY,
        coalesce(ASSET_DEBT, 0) ASSET_DEBT,
        coalesce(FUND_CASH_UP, 0) + coalesce(FUND_CASH_DOWN, 0) + coalesce(FUND_CASH_EXP, 0) + coalesce(FUND_CASH_OTHER, 0) + coalesce(FUND_VALUE_ADJ, 0) + coalesce(FUND_REMOVE_DBL, 0) as FUND_LEVEL_ADJ
    FROM {silver_prefix}aum_metrics_enriched
),
AUM AS (
    SELECT 
        REPORT_DATE,
        LAG(SUM(AUM)) OVER (ORDER BY REPORT_DATE) OPENING_AUM,
        SUM(ASSET_CMTD_EQUTY) - LAG(SUM(ASSET_CMTD_EQUTY)) OVER (ORDER BY REPORT_DATE) ASSET_CMTD_EQUTY,
        SUM(ASSET_DEBT) - LAG(SUM(ASSET_DEBT)) OVER (ORDER BY REPORT_DATE) ASSET_DEBT,
        SUM(FUND_LEVEL_ADJ) - LAG(SUM(FUND_LEVEL_ADJ)) OVER (ORDER BY REPORT_DATE) FUND_LEVEL_ADJ,
        SUM(AUM) CLOSING_AUM
    FROM raw
    GROUP BY ALL
),
TX AS (
    SELECT 
        *,
        -amount_gbp as amount_pos,
        CASE WHEN type IN ("EQ - Purchase/Subscription (w/o Commitment)", "EQ - Subscription (Capital Issue following Commitment)", "LN - Advance/Purchase (w/o commitment)", "LN - Advance (following Commitment)") THEN -amount_gbp ELSE 0 END AS CAPITAL_INJ,
        CASE WHEN type = "EQ - Sale/Redemption" THEN -amount_gbp ELSE 0 END AS DISPOSAL
    FROM {gold_prefix}aum_capital_injections_and_disposals_transactions
), 
TRX AS (
    SELECT 
        tx_effective_report_date AS REPORT_DATE,
        SUM(coalesce(CAPITAL_INJ,0)) CAPITAL_INJ,
        SUM(coalesce(DISPOSAL,0)) DISPOSAL
    FROM TX
    GROUP BY ALL
), 
SILVER_AUM_BRIDGE AS (
    SELECT 
        REPORT_DATE AS Report_Date,
        OPENING_AUM AS Previous_AUM,
        COALESCE(CAPITAL_INJ, 0) AS Asset_Capital_Injections,
        CLOSING_AUM - OPENING_AUM - COALESCE(CAPITAL_INJ, 0) - ASSET_CMTD_EQUTY - ASSET_DEBT - COALESCE(DISPOSAL, 0) - FUND_LEVEL_ADJ AS Asset_NAV_Change,
        ASSET_CMTD_EQUTY AS Asset_Committed_Equity,
        ASSET_DEBT AS Asset_Debt_Movements,
        COALESCE(DISPOSAL, 0) AS Asset_Disposals,
        FUND_LEVEL_ADJ AS Fund_Level_Adj,
        CLOSING_AUM AS Current_AUM,
        CURRENT_TIMESTAMP() AS Refresh_Timestamp
    FROM AUM
    LEFT JOIN TRX USING (REPORT_DATE)
)
SELECT
    Report_Date,
    ROUND(SUM(Previous_AUM), 3) AS Previous_AUM,
    ROUND(SUM(Asset_Capital_Injections), 3) AS Asset_Capital_Injections,
    ROUND(SUM(Asset_NAV_Change), 3) AS Asset_NAV_Change,
    ROUND(SUM(Asset_Committed_Equity), 3) AS Asset_Committed_Equity,
    ROUND(SUM(Asset_Debt_Movements), 3) AS Asset_Debt_Movements,
    ROUND(SUM(Asset_Disposals), 3) AS Asset_Disposals,
    ROUND(SUM(Fund_Level_Adj), 3) AS Fund_Level_Adj,
    ROUND(SUM(Current_AUM), 3) AS Current_AUM,
    CURRENT_TIMESTAMP() AS Refresh_Timestamp
FROM SILVER_AUM_BRIDGE
GROUP BY REPORT_DATE
ORDER BY report_date DESC
"""