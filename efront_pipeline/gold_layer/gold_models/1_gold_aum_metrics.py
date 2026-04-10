gold_aum_metrics_sql_code = """
SELECT
  m.REPORT_DATE AS Report_Date,
  'GBP' AS Currency,

  -- Fund
  COALESCE(NULLIF(m.cdm_fund, ''), NULLIF(m.ef_fund, ''), 'Unknown') AS Fund,
  COALESCE(NULLIF(m.fund_structure_type_name, ''), 'Unknown') AS Fund_Structure_Type,

  -- Portfolio
  COALESCE(NULLIF(m.cdm_portfolio, ''), NULLIF(m.ef_portfolio, ''), 'Unknown') AS Portfolio,
  COALESCE(NULLIF(m.inv_pipeline_type, ''), 'Unknown') AS Investment_Pipeline_Type,
  COALESCE(NULLIF(m.invp_investment_strategy, ''), 'Unknown') AS Investment_Strategy,

  -- Assets (as-is; raw)
  COALESCE(NULLIF(m.asset_names, ''), 'Unknown')                  AS Assets,
  COALESCE(NULLIF(m.asset_types, ''), 'Unknown')                  AS Asset_Types,
  COALESCE(NULLIF(m.asset_lifecycle_phases, ''), 'Unknown')       AS Asset_Lifecycle_Phase,
  COALESCE(NULLIF(m.asset_ownership_phases, ''), 'Unknown')       AS Asset_Ownership_Phase,
  COALESCE(NULLIF(m.asset_countries, ''), 'Unknown')              AS Asset_Countries,
  COALESCE(NULLIF(m.asset_technologies, ''), 'Unknown')           AS Asset_Technologies,

  -- Assets for Reporting (LH)
  COALESCE(NULLIF(m.reporting_asset_type, ''), 'Unknown')                  AS Reporting_Asset_Type,
  COALESCE(NULLIF(m.reporting_asset_lifecycle_phase, ''), 'Unknown')       AS Reporting_Asset_Lifecycle_Phase,
  COALESCE(NULLIF(m.reporting_asset_ownership_phase, ''), 'Unknown')       AS Reporting_Asset_Ownership_Phase,
  COALESCE(NULLIF(m.reporting_asset_country, ''), 'Unknown')               AS Reporting_Asset_Country,
  COALESCE(NULLIF(m.reporting_asset_technology, ''), 'Unknown')            AS Reporting_Asset_Technology,

  -- Metrics
  m.AUM AS AUM,
  m.ASSET_NAV AS Asset_NAV,
  m.DRY_POWDER AS Dry_Powder,
  m.FUM AS FUM,
  m.FUND_GAV AS Fund_GAV,
  m.FUND_NAV AS Fund_NAV,
  m.PORTFOLIO_GAV AS Portfolio_GAV,

  COALESCE(m.ASSET_DEBT_CMT, 0) + COALESCE(m.ASSET_EQUITY_CMT, 0) + COALESCE(m.ADJ_UNDRAWNCOMMITMENT, 0) AS `Asset_Committed_Equity`,
  COALESCE(m.EXTERNAL_DEBT_COMMIT, 0) AS `Asset_Committed_External_Debt`,
  COALESCE(m.ASSET_DEBT, 0) AS `Asset_External_Debt`,
  COALESCE(m.ASSET_LEVEL_CASH, 0) AS `Asset_Level_Cash`,
  COALESCE(m.ASSET_LEVEL_CASH_ADJ, 0) AS `Asset_Level_Cash_Adj`,

  (COALESCE(m.FUND_CASH_EXP, 0) + COALESCE(m.FUND_VALUE_ADJ, 0) + COALESCE(m.FUND_CASH_OTHER, 0)) AS `Fund_HoldCo_Cash_and_Liabilities`,

  COALESCE(m.FUND_CASH_DOWN, 0) AS `Fund_Downstream_Cash`,
  COALESCE(m.FUND_CASH_UP, 0) AS `Fund_Upstream_Cash`,
  COALESCE(m.FUND_VALUE_ADJ, 0) AS `Fund_Level_Other_Adj`,
  COALESCE(m.FUND_REMOVE_DBL, 0) AS `Fund_Remove_Double_Count`,
  COALESCE(m.TOTAL_FUND_DEBT, 0) AS `Fund_Debt`,
  COALESCE(m.UNDRAWN_FUND_DEBT, 0) AS `Fund_Undrawn_Debt`,
  COALESCE(m.FUND_CONDITIONAL_ACQ_EQ, 0) AS `Fund_Conditional_Acquisition`,
  COALESCE(m.TOTAL_FUND_CAPITAL_HR, 0) AS `Fund_Undrawn_Subscriptions`,

  m.asset_details AS Asset_Details,

  CURRENT_TIMESTAMP() AS Refresh_Timestamp
FROM {silver_prefix}aum_metrics_enriched m
ORDER BY
  COALESCE(NULLIF(m.cdm_fund, ''), NULLIF(m.ef_fund, '')),
  COALESCE(NULLIF(m.cdm_portfolio, ''), NULLIF(m.ef_portfolio, '')),
  m.REPORT_DATE DESC
"""