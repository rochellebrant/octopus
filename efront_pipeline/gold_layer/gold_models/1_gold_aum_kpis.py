gold_aum_kpis_sql_code = """
SELECT
  REPORT_DATE AS Report_Date,
  'GBP' AS Currency,
  ROUND(SUM(ASSET_NAV), 3) AS Asset_NAV,
  ROUND(SUM(AUM), 3) AS AUM,
  ROUND(SUM(DRY_POWDER), 3) AS Dry_Powder,
  ROUND(SUM(FUM), 3) AS FUM,
  ROUND(SUM(FUND_GAV), 3) AS Fund_GAV,
  ROUND(SUM(FUND_NAV), 3) AS Fund_NAV,
  ROUND(SUM(PORTFOLIO_GAV), 3) AS Portfolio_GAV,
  CURRENT_TIMESTAMP() AS Refresh_Timestamp
FROM {silver_prefix}aum_metrics_enriched
GROUP BY REPORT_DATE
ORDER BY REPORT_DATE DESC
"""