gold_aum_kpis_by_reporting_lifecycle_phase_sql_code = """
WITH StrategyTotals AS (
  SELECT
    Report_Date,
    Reporting_Asset_Lifecycle_Phase AS Lifecycle_Phase,
    SUM(AUM) AS AUM,
    SUM(Asset_NAV) AS Asset_NAV,
    SUM(Dry_Powder) AS Dry_Powder,
    SUM(FUM) AS FUM,
    SUM(Fund_GAV) AS Fund_GAV,
    SUM(Fund_NAV) AS Fund_NAV,
    SUM(Portfolio_GAV) AS Portfolio_GAV,
    SUM(Asset_Committed_Equity) AS Asset_Committed_Equity,
    SUM(Asset_Committed_External_Debt) AS Asset_Committed_External_Debt,
    SUM(Asset_External_Debt) AS Asset_External_Debt,
    SUM(Asset_Level_Cash) AS Asset_Level_Cash,
    SUM(Asset_Level_Cash_Adj) AS Asset_Level_Cash_Adj,
    SUM(Fund_HoldCo_Cash_and_Liabilities) AS Fund_HoldCo_Cash_and_Liabilities,
    SUM(Fund_Downstream_Cash) AS Fund_Downstream_Cash,
    SUM(Fund_Upstream_Cash) AS Fund_Upstream_Cash,
    SUM(Fund_Level_Other_Adj) AS Fund_Level_Other_Adj,
    SUM(Fund_Remove_Double_Count) AS Fund_Remove_Double_Count,
    SUM(Fund_Debt) AS Fund_Debt,
    SUM(Fund_Undrawn_Debt) AS Fund_Undrawn_Debt,
    SUM(Fund_Conditional_Acquisition) AS Fund_Conditional_Acquisition,
    SUM(Fund_Undrawn_Subscriptions) AS Fund_Undrawn_Subscriptions
  FROM {gold_prefix}aum_metrics
  GROUP BY Report_Date, Reporting_Asset_Lifecycle_Phase
),
ReportTotals AS (
  SELECT
    Report_Date,
    SUM(AUM) AS Total_AUM,
    SUM(Asset_NAV) AS Total_Asset_NAV,
    SUM(Dry_Powder) AS Total_Dry_Powder,
    SUM(FUM) AS Total_FUM,
    SUM(Fund_GAV) AS Total_Fund_GAV,
    SUM(Fund_NAV) AS Total_Fund_NAV,
    SUM(Portfolio_GAV) AS Total_Portfolio_GAV,
    SUM(Asset_Committed_Equity) AS Total_Asset_Committed_Equity,
    SUM(Asset_Committed_External_Debt) AS Total_Asset_Committed_External_Debt,
    SUM(Asset_External_Debt) AS Total_Asset_External_Debt,
    SUM(Asset_Level_Cash) AS Total_Asset_Level_Cash,
    SUM(Asset_Level_Cash_Adj) AS Total_Asset_Level_Cash_Adj,
    SUM(Fund_HoldCo_Cash_and_Liabilities) AS Total_Fund_HoldCo_Cash_and_Liabilities,
    SUM(Fund_Downstream_Cash) AS Total_Fund_Downstream_Cash,
    SUM(Fund_Upstream_Cash) AS Total_Fund_Upstream_Cash,
    SUM(Fund_Level_Other_Adj) AS Total_Fund_Level_Other_Adj,
    SUM(Fund_Remove_Double_Count) AS Total_Fund_Remove_Double_Count,
    SUM(Fund_Debt) AS Total_Fund_Debt,
    SUM(Fund_Undrawn_Debt) AS Total_Fund_Undrawn_Debt,
    SUM(Fund_Conditional_Acquisition) AS Total_Fund_Conditional_Acquisition,
    SUM(Fund_Undrawn_Subscriptions) AS Total_Fund_Undrawn_Subscriptions
  FROM StrategyTotals
  GROUP BY Report_Date
)
SELECT
  s.Report_Date,
  s.Lifecycle_Phase,
  ROUND(s.AUM, 1) AS AUM,
  ROUND(s.Asset_NAV, 1) AS Asset_NAV,
  ROUND(s.Dry_Powder, 1) AS Dry_Powder,
  ROUND(s.FUM, 1) AS FUM,
  ROUND(s.Fund_GAV, 1) AS Fund_GAV,
  ROUND(s.Fund_NAV, 1) AS Fund_NAV,
  ROUND(s.Portfolio_GAV, 1) AS Portfolio_GAV,
  ROUND(s.Asset_Committed_Equity, 1) AS Asset_Committed_Equity,
  ROUND(s.Asset_Committed_External_Debt, 1) AS Asset_Committed_External_Debt,
  ROUND(s.Asset_External_Debt, 1) AS Asset_External_Debt,
  ROUND(s.Asset_Level_Cash, 1) AS Asset_Level_Cash,
  ROUND(s.Asset_Level_Cash_Adj, 1) AS Asset_Level_Cash_Adj,
  ROUND(s.Fund_HoldCo_Cash_and_Liabilities, 1) AS Fund_HoldCo_Cash_and_Liabilities,
  ROUND(s.Fund_Downstream_Cash, 1) AS Fund_Downstream_Cash,
  ROUND(s.Fund_Upstream_Cash, 1) AS Fund_Upstream_Cash,
  ROUND(s.Fund_Level_Other_Adj, 1) AS Fund_Level_Other_Adj,
  ROUND(s.Fund_Remove_Double_Count, 1) AS Fund_Remove_Double_Count,
  ROUND(s.Fund_Debt, 1) AS Fund_Debt,
  ROUND(s.Fund_Undrawn_Debt, 1) AS Fund_Undrawn_Debt,
  ROUND(s.Fund_Conditional_Acquisition, 1) AS Fund_Conditional_Acquisition,
  ROUND(s.Fund_Undrawn_Subscriptions, 1) AS Fund_Undrawn_Subscriptions,

  -- Percentage Shares (Coalesced to 0)
  COALESCE(try_divide(s.AUM, r.Total_AUM) * 100, 0) AS AUM_Pct,
  COALESCE(try_divide(s.Asset_NAV, r.Total_Asset_NAV) * 100, 0) AS Asset_NAV_Pct,
  COALESCE(try_divide(s.Dry_Powder, r.Total_Dry_Powder) * 100, 0) AS Dry_Powder_Pct,
  COALESCE(try_divide(s.FUM, r.Total_FUM) * 100, 0) AS FUM_Pct,
  COALESCE(try_divide(s.Fund_GAV, r.Total_Fund_GAV) * 100, 0) AS Fund_GAV_Pct,
  COALESCE(try_divide(s.Fund_NAV, r.Total_Fund_NAV) * 100, 0) AS Fund_NAV_Pct,
  COALESCE(try_divide(s.Portfolio_GAV, r.Total_Portfolio_GAV) * 100, 0) AS Portfolio_GAV_Pct,
  COALESCE(try_divide(s.Asset_Committed_Equity, r.Total_Asset_Committed_Equity) * 100, 0) AS Asset_Committed_Equity_Pct,
  COALESCE(try_divide(s.Asset_Committed_External_Debt, r.Total_Asset_Committed_External_Debt) * 100, 0) AS Asset_Committed_External_Debt_Pct,
  COALESCE(try_divide(s.Asset_External_Debt, r.Total_Asset_External_Debt) * 100, 0) AS Asset_External_Debt_Pct,
  COALESCE(try_divide(s.Asset_Level_Cash, r.Total_Asset_Level_Cash) * 100, 0) AS Asset_Level_Cash_Pct,
  COALESCE(try_divide(s.Asset_Level_Cash_Adj, r.Total_Asset_Level_Cash_Adj) * 100, 0) AS Asset_Level_Cash_Adj_Pct,
  COALESCE(try_divide(s.Fund_HoldCo_Cash_and_Liabilities, r.Total_Fund_HoldCo_Cash_and_Liabilities) * 100, 0) AS Fund_HoldCo_Cash_and_Liabilities_Pct,
  COALESCE(try_divide(s.Fund_Downstream_Cash, r.Total_Fund_Downstream_Cash) * 100, 0) AS Fund_Downstream_Cash_Pct,
  COALESCE(try_divide(s.Fund_Upstream_Cash, r.Total_Fund_Upstream_Cash) * 100, 0) AS Fund_Upstream_Cash_Pct,
  COALESCE(try_divide(s.Fund_Level_Other_Adj, r.Total_Fund_Level_Other_Adj) * 100, 0) AS Fund_Level_Other_Adj_Pct,
  COALESCE(try_divide(s.Fund_Remove_Double_Count, r.Total_Fund_Remove_Double_Count) * 100, 0) AS Fund_Remove_Double_Count_Pct,
  COALESCE(try_divide(s.Fund_Debt, r.Total_Fund_Debt) * 100, 0) AS Fund_Debt_Pct,
  COALESCE(try_divide(s.Fund_Undrawn_Debt, r.Total_Fund_Undrawn_Debt) * 100, 0) AS Fund_Undrawn_Debt_Pct,
  COALESCE(try_divide(s.Fund_Conditional_Acquisition, r.Total_Fund_Conditional_Acquisition) * 100, 0) AS Fund_Conditional_Acquisition_Pct,
  COALESCE(try_divide(s.Fund_Undrawn_Subscriptions, r.Total_Fund_Undrawn_Subscriptions) * 100, 0) AS Fund_Undrawn_Subscriptions_Pct,

  CURRENT_TIMESTAMP() AS Refresh_Timestamp

FROM StrategyTotals s
JOIN ReportTotals r
  ON s.Report_Date = r.Report_Date
ORDER BY s.Report_Date DESC, s.Lifecycle_Phase
"""