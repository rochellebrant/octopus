# Note that we calculate this table quarterly_totals_base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_by_investor_country_quarterly_calendarised_sql_code = '''
WITH src AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    COALESCE(Investor_Country, 'Unknown') AS Investor_Country,
    date_trunc('quarter', to_date(Effective_Date)) AS q_start,  -- canonical quarter key
    Commitments,
    Called,
    Distribution
  FROM {silver_prefix}fundoperations_daily
),

-- 1) Aggregate to quarter (using the quarter-start date key)
quarterly AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Country,
    q_start,
    SUM(Commitments)            AS Commitments,
    SUM(Called)                 AS Called,
    SUM(Distribution)           AS Distribution
  FROM src
  GROUP BY Fund, Fund_Structure_Type, Fund_Status, Transaction_Currency, Investor_Country, q_start
),

-- 2) Global latest quarter across ALL funds/countries
global_end AS (
  SELECT MAX(q_start) AS max_q FROM quarterly
),

-- 3) Determine start quarter per pair
bounds AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Country,
    MIN(q_start) AS start_q
  FROM quarterly
  GROUP BY Fund, Fund_Structure_Type, Fund_Status, Transaction_Currency, Investor_Country
),

-- 4) Build full quarter calendar per pair up to the global latest quarter (3-month steps)
calendar AS (
  SELECT
    b.Fund,
    b.Fund_Structure_Type,
    b.Fund_Status,
    b.Transaction_Currency,
    b.Investor_Country,
    EXPLODE(SEQUENCE(b.start_q, ge.max_q, INTERVAL 3 MONTHS)) AS q_start
  FROM bounds b
  CROSS JOIN global_end ge
),

-- 5) Left join to fill missing quarters with zeros
filled AS (
  SELECT
    c.Fund,
    c.Fund_Structure_Type,
    c.Fund_Status,
    c.Transaction_Currency,
    c.Investor_Country,
    c.q_start,
    COALESCE(q.Commitments,  0) AS Commitments,
    COALESCE(q.Called,       0) AS Called,
    COALESCE(q.Distribution, 0) AS Distribution
  FROM calendar c
  LEFT JOIN quarterly q
    ON q.Fund = c.Fund
   AND q.Fund_Structure_Type = c.Fund_Structure_Type
   AND q.Fund_Status = c.Fund_Status
   AND q.Transaction_Currency = c.Transaction_Currency
   AND q.Investor_Country = c.Investor_Country
   AND q.q_start = c.q_start
),

-- 6) Recompute cumulatives; forward-fill timestamps for synthetic rows
quarterly_totals_base AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Country,
    CONCAT(date_format(q_start, 'yyyy'), '-Q', QUARTER(q_start)) AS Year_Quarter,

    Commitments,
    Called,
    Distribution,

    SUM(Commitments)  OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Country ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Commitments,
    SUM(Called)       OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Country ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Called,
    SUM(Distribution) OVER (
      PARTITION BY Fund, Transaction_Currency, Investor_Country ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Distribution,

    current_timestamp() AS refresh_timestamp
  FROM filled
)
SELECT
  Fund,
  Fund_Structure_Type,
  Fund_Status,
  Transaction_Currency,
  Investor_Country,
  Year_Quarter,

  Commitments,
  Called,
  Distribution,
  Cumulative_Commitments,
  Cumulative_Called,
  Cumulative_Distribution,

  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM quarterly_totals_base
ORDER BY Fund, Year_Quarter, Transaction_Currency, Investor_Country
'''