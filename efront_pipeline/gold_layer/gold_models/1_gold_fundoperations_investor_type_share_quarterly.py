# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
gold_fundoperations_investor_type_share_quarterly_sql_code = '''
WITH detail AS (
  SELECT
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Transaction_Currency,
    Investor_Type,
    Year_Quarter,
    Commitments,
    Called,
    Distribution,
    Cumulative_Commitments,
    Cumulative_Called,
    Cumulative_Distribution
  FROM {silver_prefix}fundoperations_by_investor_type_quarterly
),
totals AS (
  SELECT
    Fund,
    Transaction_Currency,
    Year_Quarter,
    Total_Commitments,
    Total_Called,
    Total_Distribution,
    Total_Cumulative_Commitments,
    Total_Cumulative_Called,
    Total_Cumulative_Distribution
  FROM {gold_prefix}fundoperations_quarterly_totals_by_fund
)
SELECT
  d.Fund,
  d.Fund_Structure_Type,
  d.Fund_Status,
  d.Transaction_Currency,
  d.Investor_Type,
  d.Year_Quarter,

  -- slice absolutes
  d.Commitments,
  d.Called,
  d.Distribution,
  d.Cumulative_Commitments,
  d.Cumulative_Called,
  d.Cumulative_Distribution,

  -- fund-period totals (for centre labels / context)
  t.Total_Commitments,
  t.Total_Called,
  t.Total_Distribution,
  t.Total_Cumulative_Commitments,
  t.Total_Cumulative_Called,
  t.Total_Cumulative_Distribution,

  -- quarterly % shares
  COALESCE(ROUND(100 * d.Commitments   / NULLIF(t.Total_Commitments, 0), 2), 0) AS Commitments_Pct,
  COALESCE(ROUND(100 * d.Called        / NULLIF(t.Total_Called, 0), 2), 0)      AS Called_Pct,
  COALESCE(ROUND(100 * d.Distribution  / NULLIF(t.Total_Distribution, 0), 2), 0) AS Distribution_Pct,

  -- cumulative % shares
  COALESCE(ROUND(100 * d.Cumulative_Commitments  / NULLIF(t.Total_Cumulative_Commitments, 0), 2), 0)
    AS Cumulative_Commitments_Pct,
  COALESCE(ROUND(100 * d.Cumulative_Called       / NULLIF(t.Total_Cumulative_Called, 0), 2), 0)
    AS Cumulative_Called_Pct,
  COALESCE(ROUND(100 * d.Cumulative_Distribution / NULLIF(t.Total_Cumulative_Distribution, 0), 2), 0)
    AS Cumulative_Distribution_Pct,

  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM detail d
JOIN totals t
  ON  t.Fund                 = d.Fund
  AND t.Transaction_Currency = d.Transaction_Currency
  AND t.Year_Quarter           = d.Year_Quarter
ORDER BY d.Fund, d.Year_Quarter, d.Transaction_Currency, d.Investor_Type
'''