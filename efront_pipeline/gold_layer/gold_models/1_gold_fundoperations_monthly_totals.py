gold_fundoperations_totals_by_fund_monthly_sql_code = """
WITH detail AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    Year_Month,
    Commitments,
    Called,
    Distribution
  FROM {silver_prefix}fundoperations_monthly
),

-- Sum monthly flows to fund-period level
fund_month AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    -- Back to a date for proper window ordering
    TO_DATE(CONCAT(Year_Month, '-01')) AS ym,
    SUM(Commitments)  AS Total_Commitments,
    SUM(Called)       AS Total_Called,
    SUM(Distribution) AS Total_Distribution
  FROM detail
  GROUP BY Fund, Fund_Structure_Type, Transaction_Currency, Year_Month
),

with_cumulative AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    DATE_FORMAT(ym, 'yyyy-MM') AS Year_Month,

    Total_Commitments,
    Total_Called,
    Total_Distribution,

    SUM(Total_Commitments)  OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Commitments,

    SUM(Total_Called)       OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Called,

    SUM(Total_Distribution) OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Distribution
  FROM fund_month
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM with_cumulative
ORDER BY Fund, Year_Month, Transaction_Currency
"""