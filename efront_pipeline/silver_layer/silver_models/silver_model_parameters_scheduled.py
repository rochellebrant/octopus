table_config = {
    #  "aum_capital_injections_and_disposals_transactions": {
    #     "level": 2,
    #     "code_type": "sql",
    #     "table_type": "replace",
    #     "file_name": "silver_aum_capital_injections_and_disposals_transactions.py",
    #     "match_keys": ["transaction_id"],
    #     "load_date_column": "refresh_timestamp",
    # },
    "company": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_company.py",
        "match_keys": ["Company_IQId"],
        "load_date_column": "refresh_timestamp",
    },
    "fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fund.py",
        "match_keys": ["ef_fund_id"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_daily": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_daily.py",
        "match_keys": ["Ef_Fund_Id", "Investor", "Transaction_Currency", "Effective_Date"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_monthly_calendarised.py",
        "match_keys": ["Ef_Fund_Id", "Investor", "Transaction_Currency", "Year_Month"],
        "load_date_column": "refresh_timestamp"
    },
    "fundoperations_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_quarterly_calendarised.py",
        "match_keys": ["Ef_Fund_Id", "Investor", "Transaction_Currency", "Year_Quarter"],
        "load_date_column": "refresh_timestamp"
    },
    "fundoperations_by_investor": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_by_investor.py",
        "match_keys": ["Investor", "Transaction_Currency", "Effective_Date"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_by_investor_type_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_by_investor_type_monthly_calendarised.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_by_investor_country_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_by_investor_country_monthly_calendarised.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_by_investor_type_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_by_investor_type_quarterly_calendarised.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    },
    "fundoperations_by_investor_country_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_fundoperations_by_investor_country_quarterly_calendarised.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    },
    "fx_vs_gbp": {
        "level": 1,
        "code_type": "notebook",
        "file_name": "silver_fx_vs_gbp",
        "notebook_params": {
            "match_keys": ["Currency", "Ref_Date"],
            "load_date_column": "refresh_timestamp",
            "save_mode": "overwrite",
            # "period_column": "REPORT_DATE"
        },
        "uniqueness_expectations": {
            "composite": [["Currency", "Ref_Date"]]
        }
    },
    "investoraccount": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_investoraccount.py",
        "match_keys": ["investor_account_id", "investor_id", "ef_fund_id"],
        "load_date_column": "refresh_timestamp",
    },
    "track_record_fund": {
        "level": 2,
        "code_type": "notebook",
        "file_name": "silver_track_record_fund",
        "notebook_params": {
            "load_date_column": "refresh_timestamp",
            "save_mode": "overwrite",
        },
        "uniqueness_expectations": {
            "composite": [["ef_fund", "metric"]]
        }
    },
    "track_record_metric_dictionary": {
        "level": 1,
        "code_type": "notebook",
        "file_name": "silver_track_record_metric_dictionary",
        "notebook_params": {
            "load_date_column": "refresh_timestamp",
            "save_mode": "overwrite",
        },
        "uniqueness_expectations": {
            "composite": [["domain", "source_system_id", "dataset_name", "metric"]]			
        }
    },
    "transactions": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_transactions.py",
        "match_keys": ["transaction_id"],
        "load_date_column": "refresh_timestamp",
    },
}