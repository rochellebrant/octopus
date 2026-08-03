gold_aum_kpis_by_fund_this_qtr_vs_last_qtr_sql_code = """
WITH report_dates AS (
  SELECT DISTINCT Report_Date
  FROM {gold_prefix}aum_metrics
),
ordered AS (
  SELECT
    Report_Date,
    LAG(Report_Date) OVER (ORDER BY Report_Date) AS prev_report_date
  FROM report_dates
),
pairs AS (
  SELECT
    prev_report_date AS prev_period,
    Report_Date      AS this_period
  FROM ordered
  WHERE prev_report_date IS NOT NULL
),
-- 1) Aggregate all KPIs within (Fund, Report_Date)
aggregated AS (
  SELECT
    t.Fund,
    t.Report_Date,
    SUM(t.AUM)           AS AUM,
    SUM(t.Dry_Powder)    AS Dry_Powder,
    SUM(t.FUM)           AS FUM,
    SUM(t.Fund_GAV)      AS Fund_GAV,
    SUM(t.Fund_NAV)      AS Fund_NAV,
    SUM(t.Portfolio_GAV) AS Portfolio_GAV
  FROM {gold_prefix}aum_metrics t
  GROUP BY t.Fund, t.Report_Date
),
-- 2) UNPIVOT the aggregated measures
metrics AS (
  SELECT Fund, Report_Date, 'AUM'            AS metric, AUM            AS value FROM aggregated
  UNION ALL
  SELECT Fund, Report_Date, 'Dry Powder'     AS metric, Dry_Powder     AS value FROM aggregated
  UNION ALL
  SELECT Fund, Report_Date, 'FUM'            AS metric, FUM            AS value FROM aggregated
  UNION ALL
  SELECT Fund, Report_Date, 'Fund GAV'       AS metric, Fund_GAV       AS value FROM aggregated
  UNION ALL
  SELECT Fund, Report_Date, 'Fund NAV'       AS metric, Fund_NAV       AS value FROM aggregated
  UNION ALL
  SELECT Fund, Report_Date, 'Portfolio GAV'  AS metric, Portfolio_GAV  AS value FROM aggregated
),
-- 3) Join each (prev, this) period pair to metric values and pivot to prev/this
joined AS (
  SELECT
    p.prev_period,
    p.this_period,
    m.Fund,
    m.metric,
    MAX(CASE WHEN m.Report_Date = p.prev_period THEN m.value END) AS prev_value,
    MAX(CASE WHEN m.Report_Date = p.this_period THEN m.value END) AS this_value
  FROM pairs p
  JOIN metrics m
    ON m.Report_Date IN (p.prev_period, p.this_period)
  GROUP BY p.prev_period, p.this_period, m.Fund, m.metric
)
SELECT
  j.Fund,
  j.metric                                           AS `Metric`,
  j.prev_period                                      AS `Prev_Report_Date`,
  j.this_period                                      AS `This_Report_Date`,
  ROUND(j.prev_value, 0)                             AS `KPI_Prev_Report_Date`,
  ROUND(j.this_value, 0)                             AS `KPI_This_Report_Date`,
  ROUND(j.this_value - j.prev_value, 0)              AS `Delta`,
  COALESCE(
    ROUND(
      CASE
        WHEN j.prev_value IS NULL OR j.prev_value = 0 THEN NULL
        ELSE (j.this_value - j.prev_value) / j.prev_value * 100
      END, 2
    ),
    0
  )                                                  AS `Percentage_Delta`,
  CURRENT_TIMESTAMP()                                AS `Refresh_Timestamp`
FROM joined j
-- OPTIONAL: filter to a specific "this" period (e.g., 2024-09-30)
-- WHERE j.this_period = DATE '2024-09-30'
ORDER BY j.Fund, j.this_period DESC, j.metric
"""