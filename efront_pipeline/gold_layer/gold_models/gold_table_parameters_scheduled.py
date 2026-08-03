tables_config = {
    "aum_kpis": {
        "level": 1,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_aum_kpis.py",
        "match_keys": ["Report_Date"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_metrics": {
        "level": 1,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_aum_metrics.py",
        "match_keys": ["Fund", "Portfolio", "Report_Date"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_country": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "2_gold_aum_kpis_by_reporting_country.py",
        "match_keys": ["Report_Date", "Country"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_lifecycle_phase": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "2_gold_aum_kpis_by_reporting_lifecycle_phase.py",
        "match_keys": ["Report_Date", "Lifecycle_Phase"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_technology": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "2_gold_aum_kpis_by_reporting_technology.py",
        "match_keys": ["Report_Date", "Technology"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_vs_prev": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "2_gold_aum_kpis_vs_prev.py",
        "match_keys": ["This_Report_Date", "Metric"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_fund_vs_prev": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "2_gold_aum_kpis_by_fund_vs_prev.py",
        "match_keys": ["This_Report_Date", "Metric", "Fund"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_bridge": {
        "level": 1,
        "code_type": "sql",
        "file_name": "0_gold_aum_bridge.py",
        "match_keys": ["Report_Date",],
        "load_date_column": "Refresh_Timestamp",
    }
    , "track_record_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_track_record_fund.py",
        # "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }

    , "fundoperations_monthly_totals_by_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_monthly_totals.py",
        "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_quarterly_totals_by_fund": {
        "level": 1,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_quarterly_totals.py",
        "match_keys": ["Fund", "Fund_Structure_Type", "Transaction_Currency", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_country_share_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_investor_country_share_monthly.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_country_share_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_investor_country_share_quarterly.py",
        "match_keys": ["Fund", "Investor_Country", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_type_share_monthly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_investor_type_share_monthly.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Month"],
        "load_date_column": "refresh_timestamp",
    }
    , "fundoperations_investor_type_share_quarterly": {
        "level": 2,
        "code_type": "sql",
        "table_type": "overwrite_in_place",
        "file_name": "1_gold_fundoperations_investor_type_share_quarterly.py",
        "match_keys": ["Fund", "Investor_Type", "Year_Quarter"],
        "load_date_column": "refresh_timestamp",
    }
}