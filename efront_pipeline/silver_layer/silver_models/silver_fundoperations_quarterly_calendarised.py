# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_quarterly_sql_code = '''
WITH src AS (
  SELECT
    Ef_Fund_Id,
    Ef_Fund,
    Ef_Fund_Long,
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor,
    Investor_Type,
    Investor_Country,
    date_trunc('quarter', to_date(Effective_Date)) AS q_start,  -- canonical quarter key
    Commitments,
    Called,
    Distribution,
    NAV
  FROM {silver_prefix}fundoperations_daily
),

-- 1) Aggregate to quarter (using the quarter-start date key)
quarterly AS (
  SELECT
    Ef_Fund_Id,
    Ef_Fund,
    Ef_Fund_Long,
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor,
    Investor_Type,
    Investor_Country,
    q_start,
    SUM(Commitments)            AS Commitments,
    SUM(Called)                 AS Called,
    SUM(Distribution)           AS Distribution,
    MAX(NAV)                          AS NAV
  FROM src
  GROUP BY
    Ef_Fund_Id, Ef_Fund, Ef_Fund_Long, Fund, Fund_Structure_Type, Fund_Status,
    Transaction_Currency, Investor, Investor_Type, Investor_Country, q_start
),

-- 2) Global latest quarter across ALL funds
global_end AS (
  SELECT MAX(q_start) AS max_q FROM quarterly
),

-- 3) Determine start quarter per full dimension combo
bounds AS (
  SELECT
    Ef_Fund_Id,
    Ef_Fund,
    Ef_Fund_Long,
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor,
    Investor_Type,
    Investor_Country,
    MIN(q_start) AS start_q
  FROM quarterly
  GROUP BY
    Ef_Fund_Id, Ef_Fund, Ef_Fund_Long, Fund, Fund_Structure_Type, Fund_Status,
    Transaction_Currency, Investor, Investor_Type, Investor_Country
),

-- 4) Build full quarter calendar per combo up to the global latest quarter (3-month steps)
calendar AS (
  SELECT
    b.Ef_Fund_Id,
    b.Ef_Fund,
    b.Ef_Fund_Long,
    b.Fund,
    b.Fund_Structure_Type,
    b.Fund_Status,
    b.Transaction_Currency,
    b.Investor,
    b.Investor_Type,
    b.Investor_Country,
    EXPLODE(SEQUENCE(b.start_q, ge.max_q, INTERVAL 3 MONTHS)) AS q_start
  FROM bounds b
  CROSS JOIN global_end ge
),

-- 5) Left join to fill missing quarters with zeros
filled AS (
  SELECT
    c.Ef_Fund_Id,
    c.Ef_Fund,
    c.Ef_Fund_Long,
    c.Fund,
    c.Fund_Structure_Type,
    c.Fund_Status,
    c.Transaction_Currency,
    c.Investor,
    c.Investor_Type,
    c.Investor_Country,
    c.q_start,
    COALESCE(q.Commitments,  0) AS Commitments,
    COALESCE(q.Called,       0) AS Called,
    COALESCE(q.Distribution, 0) AS Distribution,
    q.NAV
  FROM calendar c
  LEFT JOIN quarterly q
    ON  q.Ef_Fund_Id            <=> c.Ef_Fund_Id
    AND q.Ef_Fund              <=> c.Ef_Fund
    AND q.Ef_Fund_Long         <=> c.Ef_Fund_Long
    AND q.Fund                 <=> c.Fund
    AND q.Fund_Structure_Type  <=> c.Fund_Structure_Type
    AND q.Fund_Status          <=> c.Fund_Status
    AND q.Transaction_Currency <=> c.Transaction_Currency
    AND q.Investor             <=> c.Investor
    AND q.Investor_Type        <=> c.Investor_Type
    AND q.Investor_Country     <=> c.Investor_Country
    AND q.q_start              <=> c.q_start
),

-- 6) Recompute cumulatives; optional forward-fill for NAV and timestamp
final AS (
  SELECT
    Ef_Fund_Id,
    Ef_Fund,
    Ef_Fund_Long,
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor,
    Investor_Type,
    Investor_Country,
    CONCAT(date_format(q_start, 'yyyy'), '-Q', QUARTER(q_start)) AS Year_Quarter,
    q_start                                                       AS quarter_start,
    Commitments,
    Called,
    Distribution,

    SUM(Commitments)  OVER (
      PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
      ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Commitments,

    SUM(Called)       OVER (
      PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
      ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Called,

    SUM(Distribution) OVER (
      PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
      ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Distribution,

    -- Forward-fill NAV if desired (comment out the COALESCE/last_value if you want sparse NAVs)
    LAST_VALUE(NAV, TRUE) OVER (
      PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
      ORDER BY q_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS NAV,

    current_timestamp() AS refresh_timestamp
  FROM filled
)

SELECT
  Ef_Fund_Id,
  Ef_Fund,
  Ef_Fund_Long,
  Fund,
  Fund_Structure_Type,
  Fund_Status,
  Investor,
  Investor_Type,
  Investor_Country,
  Year_Quarter,
  Transaction_Currency,
  Commitments,
  Called,
  Distribution,
  (Cumulative_Commitments - Cumulative_Called) AS Remaining_Commitments,
  Cumulative_Commitments,
  Cumulative_Called,
  Cumulative_Distribution,
  NAV,
  refresh_timestamp
FROM final
ORDER BY Fund, Investor, Investor_Type, Investor_Country, quarter_start
'''