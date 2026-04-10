# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_by_investor_type_monthly_calendarised_sql_code = '''
WITH src AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    COALESCE(Investor_Type,'Unknown') AS Investor_Type,
    TRUNC(TO_DATE(Effective_Date), 'MM') AS ym,
    Commitments,
    Called,
    Distribution
  FROM {silver_prefix}fundoperations_daily
),
monthly AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Type,
    ym,
    SUM(Commitments)            AS Commitments,
    SUM(Called)                 AS Called,
    SUM(Distribution)           AS Distribution
  FROM src
  GROUP BY Fund, Fund_Structure_Type, Fund_Status, Transaction_Currency, Investor_Type, ym
),
-- global latest month across ALL funds/investor types
global_end AS (
  SELECT MAX(ym) AS max_ym FROM monthly
),
bounds AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Type,
    MIN(ym) AS start_ym
  FROM monthly
  GROUP BY Fund, Fund_Structure_Type, Fund_Status, Transaction_Currency, Investor_Type
),
-- build a calendar from each pair's first month through the global latest month
calendar AS (
  SELECT
    b.Fund,
    b.Fund_Structure_Type,
    b.Fund_Status,
    b.Transaction_Currency,
    b.Investor_Type,
    EXPLODE(SEQUENCE(b.start_ym, ge.max_ym, INTERVAL 1 MONTH)) AS ym
  FROM bounds b
  CROSS JOIN global_end ge
),
filled AS (
  SELECT
    c.Fund, c.Fund_Structure_Type, c.Fund_Status, c.Transaction_Currency, c.Investor_Type, c.ym,
    COALESCE(m.Commitments,  0) AS Commitments,
    COALESCE(m.Called,       0) AS Called,
    COALESCE(m.Distribution, 0) AS Distribution
  FROM calendar c
  LEFT JOIN monthly m
    ON m.Fund = c.Fund
   AND m.Fund_Structure_Type = c.Fund_Structure_Type
   AND m.Fund_Status = c.Fund_Status
   AND m.Transaction_Currency = c.Transaction_Currency
   AND m.Investor_Type = c.Investor_Type
   AND m.ym = c.ym
),
detail AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Type,
    DATE_FORMAT(ym, 'yyyy-MM') AS Year_Month,

    -- monthly flows per slice
    Commitments,
    Called,
    Distribution,

    -- cumulative per slice (ordered by month)
    SUM(Commitments)  OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Type
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Commitments,

    SUM(Called)       OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Type
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Called,

    SUM(Distribution) OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Type
      ORDER BY ym
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Distribution
  FROM filled
)

SELECT
  d.*,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM detail d
ORDER BY
  Fund, Year_Month, Transaction_Currency, Investor_Type
'''