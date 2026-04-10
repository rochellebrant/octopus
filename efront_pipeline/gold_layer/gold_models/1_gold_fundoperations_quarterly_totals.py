gold_fundoperations_totals_by_fund_quarterly_sql_code = """
WITH detail AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    Year_Quarter, -- e.g. '2025-Q4'
    Commitments,
    Called,
    Distribution
  FROM {silver_prefix}fundoperations_quarterly
),

-- Aggregate flows at Fund-Quarter level
fund_quarter AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    Year_Quarter,
    SUM(Commitments)  AS Total_Commitments,
    SUM(Called)       AS Total_Called,
    SUM(Distribution) AS Total_Distribution
  FROM detail
  GROUP BY Fund, Fund_Structure_Type, Transaction_Currency, Year_Quarter
),

-- Add cumulative totals ordered by Year_Quarter
with_cumulative AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Transaction_Currency,
    Year_Quarter,

    Total_Commitments,
    Total_Called,
    Total_Distribution,

    SUM(Total_Commitments) OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY CAST(SPLIT(Year_Quarter, '-')[0] AS INT),
               CAST(SUBSTRING(SPLIT(Year_Quarter, '-')[1], 2) AS INT)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Commitments,

    SUM(Total_Called) OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY CAST(SPLIT(Year_Quarter, '-')[0] AS INT),
               CAST(SUBSTRING(SPLIT(Year_Quarter, '-')[1], 2) AS INT)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Called,

    SUM(Total_Distribution) OVER (
      PARTITION BY Fund, Fund_Structure_Type, Transaction_Currency
      ORDER BY CAST(SPLIT(Year_Quarter, '-')[0] AS INT),
               CAST(SUBSTRING(SPLIT(Year_Quarter, '-')[1], 2) AS INT)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Total_Cumulative_Distribution
  FROM fund_quarter
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM with_cumulative
ORDER BY Fund,
         CAST(SPLIT(Year_Quarter, '-')[0] AS INT),
         CAST(SUBSTRING(SPLIT(Year_Quarter, '-')[1], 2) AS INT),
         Transaction_Currency
"""