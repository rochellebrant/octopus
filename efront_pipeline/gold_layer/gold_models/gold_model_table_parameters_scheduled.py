table_config = {
    "fundoperations_monthly_totals_by_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_monthly_totals.py",
        "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_quarterly_totals_by_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_quarterly_totals.py",
        "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
    , "track_record_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_track_record_fund.py",
        # "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }

    , "fundoperations_investor_country_share_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_investor_country_share_monthly.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_country_share_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_investor_country_share_quarterly.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_type_share_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_investor_type_share_monthly.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_type_share_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_fundoperations_investor_type_share_quarterly.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
}