table_config = {
    "aum_fund_quarterly_snapshots": {
        "endpoint": "/api/reporting/aum_fund",
        "match_keys": ["REPORT_DATE", "FUND_SHORT"],
        "period_column": "REPORT_DATE",
        "save_mode": "merge",
        "column_order": [
            "REPORT_DATE", "FUND_SHORT", "FUND_CASH_UP", "FUND_CASH_DOWN", "FUND_CASH_EXP", 
            "FUND_CAPITAL_DR", "FUND_CASH_OTHER", "FUND_VALUE_ADJ", "FUND_REMOVE_DBL", 
            "FUND_CONDITIONAL_ACQ_EQ", "TOTAL_FUND_DEBT", "UNDRAWN_FUND_DEBT", 
            "TOTAL_COMMITTED", "EXPIRED_COMMITMENT", "TOTAL_CALLED", "RETURN_OF_CALL", 
            "TOTAL_RECALLED", "TOTAL_FUND_CAPITAL_HR", "FUND_NAV", "FUM", "FUND_GAV", 
            "FUND_AUM", "DRY_POWDER", "datalake_ingestion_timestamp"
        ]
    },
    "aum_asset_quarterly_snapshots": {
        "endpoint": "/api/reporting/aum_asset",
        "match_keys": ["REPORT_DATE", "FUND_SHORT", "PORTFOLIO_NAME", "COMPANY_NAME"],
        "period_column": "REPORT_DATE",
        "save_mode": "merge",
        "column_order": [
            "REPORT_DATE", "COMPANY_NAME", "PORTFOLIO_NAME", "FUND_SHORT", "EQ_VALUATION", 
            "LN_VALUATION", "LOAN_BALANCE", "ASSET_DEBT", "ASSET_DEBT_CMT", "ASSET_EQUITY_CMT", 
            "EXTERNAL_DEBT_COMMIT", "ADJ_UNDRAWNCOMMITMENT", "ASSET_LEVEL_CASH", 
            "ASSET_LEVEL_CASH_ADJ", "ASSET_NAV", "PORTFOLIO_GAV", "AUM", 
            "ASSET_DRY_POWDER", "datalake_ingestion_timestamp"
        ]
    },
}
