gold_aum_kpis_this_qtr_vs_last_qtr_sql_code = """
WITH report_dates AS (
  SELECT DISTINCT Report_Date
  FROM {gold_prefix}aum_kpis
),
ordered AS (
  SELECT
    Report_Date,
    LAG(Report_Date) OVER (ORDER BY Report_Date) AS prev_report_date
  FROM report_dates
),
pairs AS (
  SELECT
    Report_Date       AS this_period,
    prev_report_date  AS prev_period
  FROM ordered
  WHERE prev_report_date IS NOT NULL
),
unpivot AS (
  SELECT
    t.Report_Date,
    STACK(7,
      'Asset NAV',     t.Asset_NAV,
      'AUM',           t.AUM,
      'Dry Powder',    t.Dry_Powder,
      'FUM',           t.FUM,
      'Fund GAV',      t.Fund_GAV,
      'Fund NAV',      t.Fund_NAV,
      'Portfolio GAV', t.Portfolio_GAV
    ) AS (Metric, Value)
  FROM {gold_prefix}aum_kpis t
),
joined AS (
  SELECT
    p.prev_period,
    p.this_period,
    u.Metric,
    MAX(CASE WHEN u.Report_Date = p.prev_period THEN u.Value END) AS prev_value,
    MAX(CASE WHEN u.Report_Date = p.this_period THEN u.Value END) AS this_value
  FROM pairs p
  JOIN unpivot u
    ON u.Report_Date IN (p.prev_period, p.this_period)
  GROUP BY p.prev_period, p.this_period, u.Metric
)
SELECT
  prev_period AS `Prev_Report_Date`,
  this_period AS `This_Report_Date`,
  Metric      AS `Metric`,
  ROUND(prev_value, 0)                                         AS `KPI_Prev_Report_Date`,
  ROUND(this_value, 0)                                         AS `KPI_This_Report_Date`,
  ROUND(this_value - prev_value, 0)                            AS `Delta`,
  COALESCE(
    ROUND(
      CASE WHEN prev_value IS NULL OR prev_value = 0
           THEN NULL
           ELSE (this_value - prev_value) / prev_value * 100
      END, 2
    ),
    0
  )                                                            AS `Percentage_Delta`,
  CURRENT_TIMESTAMP()                                          AS `Refresh_Timestamp`
FROM joined
ORDER BY this_period DESC, UPPER(TRIM(Metric))
"""