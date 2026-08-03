silver_table_rules = {
    "aum_metrics_enriched": {
        "_CHECK_ASSET_NAV_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_ASSET_NAV, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_FUND_NAV_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_FUND_NAV, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_FUM_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_FUM, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_PORTFOLIO_GAV_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_PORTFOLIO_GAV, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_FUND_GAV_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_FUND_GAV, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_AUM_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_AUM, 0)) > 1",
            "hard_check": False,
            },
        "_CHECK_DRY_POWDER_abs_exceeds_1": {
            "expr": "ABS(COALESCE(CHECK_DRY_POWDER, 0)) > 1",
            "hard_check": False,
            }
    },
    "datapointsexport_trackrecord": {
        "_CATEGORY_NAME_is_not_trackrecord": "lower(CATEGORY_NAME) NOT LIKE 'track tecord%'"
    },
    "datapointsexport_valuationbridge": {
        "_CATEGORY_NAME_is_not_valuationbridge": "lower(CATEGORY_NAME) NOT LIKE 'valuation bridge%'"
    },
    "fundoperations_daily": {
        "_ef_Fund_is_empty_string": "TRIM(ef_Fund) = ''",
        "_Fund_is_empty_string": "TRIM(Fund) = ''",
        "_Investor_is_empty_string": "TRIM(Investor) = ''",
        "_Investor_Type_is_empty_string": "TRIM(Investor_Type) = ''",
        "_Investor_Country_is_empty_string": "TRIM(Investor_Country) = ''",
        "_Effective_Date_is_empty_string": "TRIM(Effective_Date) = ''",
    },
    "fx_vs_gbp": {
        "_Currency_is_empty_string": "TRIM(Currency) = ''",
    },
}