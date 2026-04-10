gold_layer_definitions = {
    "aum_bridge": {
        "table": {
            "comment": "Gold layer reporting table for the AUM Waterfall. Provides a period-on-period bridge of total AUM movements, aggregated and rounded for use in executive dashboards and financial statements."
        },
        "columns": {
            "Report_Date": {"schema": {"data_type": "DATE"}, "description": {"comment": "The reporting date for the AUM movement period.", "long_name": "Report Date", "unit": "n/a"}},
            "Previous_AUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "The total opening AUM balance from the start of the reporting period.", "long_name": "Total Opening AUM", "unit": "GBP"}},
            "Asset_Capital_Injections": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated capital deployed into assets.", "long_name": "Total Capital Injections", "unit": "GBP"}},
            "Asset_NAV_Change": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated movement in asset market valuations.", "long_name": "Total Asset NAV Performance", "unit": "GBP"}},
            "Asset_Committed_Equity": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated change in committed but undrawn capital.", "long_name": "Total Committed Equity Movement", "unit": "GBP"}},
            "Asset_Debt_Movements": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated change in asset-level debt balances.", "long_name": "Total Asset Debt Movement", "unit": "GBP"}},
            "Asset_Disposals": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated capital returned through asset sales.", "long_name": "Total Asset Disposals", "unit": "GBP"}},
            "Fund_Level_Adj": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated movements in fund-level cash and expenses.", "long_name": "Total Fund Level Adjustments", "unit": "GBP"}},
            "Current_AUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "The total closing AUM balance.", "long_name": "Total Closing AUM", "unit": "GBP"}},
            "Refresh_Timestamp": {"schema": {"data_type": "TIMESTAMP"}, "description": {"comment": "Timestamp when this Gold model was refreshed.", "long_name": "Gold Model Refresh Timestamp", "unit": "n/a"}}
        }
    },

    "aum_kpis": {
        "table": {
            "comment": "Gold layer KPI table for Assets Under Management (AUM) metrics. Aggregates financial indicators including NAV, AUM, FUM, and Dry Powder at a reporting date level, converted to GBP."
        },
        "columns": {
            "Report_Date": {"schema": {"data_type": "DATE"}, "description": {"comment": "The reporting date for the aggregated AUM metrics.", "long_name": "Report Date", "unit": "n/a"}},
            "Currency": {"schema": {"data_type": "STRING"}, "description": {"comment": "The reporting currency (hardcoded to GBP).", "long_name": "Reporting Currency", "unit": "n/a"}},
            "Asset_NAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Net Asset Value at the asset level.", "long_name": "Total Asset NAV", "unit": "GBP"}},
            "AUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Assets Under Management for the period.", "long_name": "Total AUM", "unit": "GBP"}},
            "Dry_Powder": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Aggregated undrawn capital commitments.", "long_name": "Total Dry Powder", "unit": "GBP"}},
            "FUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Funds Under Management.", "long_name": "Total FUM", "unit": "GBP"}},
            "Fund_GAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Gross Asset Value at the fund level.", "long_name": "Total Fund GAV", "unit": "GBP"}},
            "Fund_NAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Net Asset Value at the fund level.", "long_name": "Total Fund NAV", "unit": "GBP"}},
            "Portfolio_GAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total Gross Asset Value of the portfolio.", "long_name": "Total Portfolio GAV", "unit": "GBP"}},
            "Refresh_Timestamp": {"schema": {"data_type": "TIMESTAMP"}, "description": {"comment": "Timestamp when this Gold KPI model was calculated.", "long_name": "Gold Model Refresh Timestamp", "unit": "n/a"}}
        }
    },

    "aum_metrics": {
        "table": {
            "comment": "Gold layer reporting table providing a granular breakdown of AUM, NAV, and liquidity metrics across Funds, Portfolios, and Assets."
        },
        "columns": {
            "Report_Date": {"schema": {"data_type": "DATE"}, "description": {"comment": "The reporting date.", "long_name": "Report Date", "unit": "n/a"}},
            "Currency": {"schema": {"data_type": "STRING"}, "description": {"comment": "Reporting currency.", "long_name": "Reporting Currency", "unit": "n/a"}},
            "Fund": {"schema": {"data_type": "STRING"}, "description": {"comment": "Fund name.", "long_name": "Fund Name", "unit": "n/a"}},
            "Fund_Structure_Type": {"schema": {"data_type": "STRING"}, "description": {"comment": "Legal structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"}},
            "Portfolio": {"schema": {"data_type": "STRING"}, "description": {"comment": "Portfolio/Project name.", "long_name": "Portfolio Name", "unit": "n/a"}},
            "Investment_Pipeline_Type": {"schema": {"data_type": "STRING"}, "description": {"comment": "Classification in pipeline.", "long_name": "Investment Pipeline Type", "unit": "n/a"}},
            "Investment_Strategy": {"schema": {"data_type": "STRING"}, "description": {"comment": "Investment strategy.", "long_name": "Investment Strategy", "unit": "n/a"}},
            "Assets": {"schema": {"data_type": "STRING"}, "description": {"comment": "Raw concatenated asset names.", "long_name": "Asset Names (Raw)", "unit": "n/a"}},
            "AUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total AUM.", "long_name": "AUM", "unit": "GBP"}},
            "Asset_NAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Net Asset Value (Asset level).", "long_name": "Asset NAV", "unit": "GBP"}},
            "Dry_Powder": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Undrawn capital.", "long_name": "Dry Powder", "unit": "GBP"}},
            "FUM": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Funds Under Management.", "long_name": "FUM", "unit": "GBP"}},
            "Fund_GAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Fund GAV.", "long_name": "Fund GAV", "unit": "GBP"}},
            "Fund_NAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Fund NAV.", "long_name": "Fund NAV", "unit": "GBP"}},
            "Portfolio_GAV": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Portfolio GAV.", "long_name": "Portfolio GAV", "unit": "GBP"}},
            "Asset_Committed_Equity": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total committed equity.", "long_name": "Asset Committed Equity", "unit": "GBP"}},
            "Asset_Committed_External_Debt": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Committed external debt.", "long_name": "Asset Committed External Debt", "unit": "GBP"}},
            "Asset_External_Debt": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Current external debt.", "long_name": "Asset External Debt", "unit": "GBP"}},
            "Asset_Level_Cash": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Cash at asset level.", "long_name": "Asset Level Cash", "unit": "GBP"}},
            "Fund_HoldCo_Cash_and_Liabilities": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Fund HoldCo Cash and Liabilities.", "long_name": "Fund HoldCo Cash and Liabilities", "unit": "GBP"}},
            "Refresh_Timestamp": {"schema": {"data_type": "TIMESTAMP"}, "description": {"comment": "Refresh timestamp.", "long_name": "Gold Model Refresh Timestamp", "unit": "n/a"}}
        }
    },

    "aum_kpis_vs_prev": {
        "table": {
            "comment": "Gold layer comparison table analyzing key performance indicators (KPIs) across consecutive reporting periods."
        },
        "columns": {
            "Prev_Report_Date": {"schema": {"data_type": "DATE"}, "description": {"comment": "Previous reporting date.", "long_name": "Previous Report Date", "unit": "n/a"}},
            "This_Report_Date": {"schema": {"data_type": "DATE"}, "description": {"comment": "Current reporting date.", "long_name": "Current Report Date", "unit": "n/a"}},
            "Metric": {"schema": {"data_type": "STRING"}, "description": {"comment": "KPI Metric Name.", "long_name": "KPI Metric Name", "unit": "n/a"}},
            "KPI_Prev_Report_Date": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Value at previous date.", "long_name": "Previous KPI Value", "unit": "GBP"}},
            "KPI_This_Report_Date": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Value at current date.", "long_name": "Current KPI Value", "unit": "GBP"}},
            "Delta": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Absolute change (This - Prev).", "long_name": "Absolute Movement (Delta)", "unit": "GBP"}},
            "Percentage_Delta`": {"schema": {"data_type": "DOUBLE"}, "description": {"comment": "Percentage change.", "long_name": "Percentage Movement (% Delta)", "unit": "percent"}},
            "Refresh_Timestamp": {"schema": {"data_type": "TIMESTAMP"}, "description": {"comment": "Refresh timestamp.", "long_name": "Gold Model Refresh Timestamp", "unit": "n/a"}}
        }
    },

}