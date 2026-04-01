# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_monthly_sql_code = '''
WITH
-- 1) Monthly facts (one row per combo x Year_Month)
m AS (
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
    date_format(date_trunc('month', Effective_Date), 'yyyy-MM') AS Year_Month,
    SUM(Commitments)          AS Commitments,
    SUM(Called)               AS Called,
    SUM(Distribution)         AS Distribution,
    MAX(NAV)                  AS NAV
  FROM {silver_prefix}fundoperations_daily
  GROUP BY
    Ef_Fund_Id, Ef_Fund, Ef_Fund_Long, Fund, Fund_Structure_Type, Fund_Status,
    Transaction_Currency, Investor, Investor_Type, Investor_Country,
    date_trunc('month', Effective_Date)
),

-- 2) Per-combo first month and global last month
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
    -- first active month for the combo
    to_date(concat(MIN(Year_Month), '-01')) AS min_month_date
  FROM m
  GROUP BY
    Ef_Fund_Id, Ef_Fund, Ef_Fund_Long, Fund, Fund_Structure_Type, Fund_Status,
    Transaction_Currency, Investor, Investor_Type, Investor_Country
),
global_last AS (
  SELECT to_date(concat(MAX(Year_Month), '-01')) AS max_month_date FROM m
),

-- 3) Calendar spine: every month from combo's first month to global last
spine AS (
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
    date_format(months, 'yyyy-MM') AS Year_Month
  FROM bounds b
  CROSS JOIN global_last g
  LATERAL VIEW explode(sequence(b.min_month_date, g.max_month_date, interval 1 month)) s AS months
),

-- 4) Join facts to spine; fill missing months with zeros (NAV left as-is)
joined AS (
  SELECT
    s.Ef_Fund_Id,
    s.Ef_Fund,
    s.Ef_Fund_Long,
    s.Fund,
    s.Fund_Structure_Type,
    s.Fund_Status,
    s.Transaction_Currency,
    s.Investor,
    s.Investor_Type,
    s.Investor_Country,
    s.Year_Month,
    coalesce(m.Commitments, 0)  AS Commitments,
    coalesce(m.Called, 0)       AS Called,
    coalesce(m.Distribution, 0) AS Distribution,
    m.NAV
  FROM spine s
  LEFT JOIN m
    ON  s.Ef_Fund_Id           <=> m.Ef_Fund_Id
    AND s.Ef_Fund             <=> m.Ef_Fund
    AND s.Ef_Fund_Long        <=> m.Ef_Fund_Long
    AND s.Fund                <=> m.Fund
    AND s.Fund_Status         <=> m.Fund_Status
    AND s.Fund_Structure_Type <=> m.Fund_Structure_Type
    AND s.Transaction_Currency<=> m.Transaction_Currency
    AND s.Investor            <=> m.Investor
    AND s.Investor_Type       <=> m.Investor_Type
    AND s.Investor_Country    <=> m.Investor_Country
    AND s.Year_Month          <=> m.Year_Month
),

-- 5) Recompute running totals on the calendarised series
calc_base1 AS (
  SELECT
    j.*,
    SUM(Commitments)  OVER (PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
                                  ORDER BY Year_Month
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Cumulative_Commitments,
    SUM(Called)       OVER (PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
                                  ORDER BY Year_Month
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Cumulative_Called,
    SUM(Distribution) OVER (PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
                                  ORDER BY Year_Month
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Cumulative_Distribution
  FROM joined j
)

SELECT
  c1.Ef_Fund_Id,
  c1.Ef_Fund,
  c1.Ef_Fund_Long,
  c1.Fund,
  c1.Fund_Structure_Type,
  c1.Fund_Status,
  c1.Investor,
  c1.Investor_Type,
  c1.Investor_Country,
  c1.Year_Month,
  c1.Transaction_Currency,
  c1.Commitments,
  c1.Called,
  c1.Distribution,
  -- Forward-fill NAV
  last(NAV, true) OVER (
    PARTITION BY Ef_Fund_Id, Transaction_Currency, Investor, Investor_Type, Investor_Country
    ORDER BY Year_Month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS NAV,
  -- c1.NAV,
  c1.Cumulative_Commitments,
  c1.Cumulative_Called,
  (c1.Cumulative_Commitments - c1.Cumulative_Called) AS Remaining_Commitments,
  c1.Cumulative_Distribution,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM calc_base1 c1
ORDER BY c1.Fund, c1.Investor, c1.Investor_Type, c1.Investor_Country, c1.Year_Month
'''