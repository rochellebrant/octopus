table_config = {
    "aum_kpis": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_aum_kpis.py",
        "match_keys": ["Report_Date"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_metrics": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_aum_metrics.py",
        "match_keys": ["Fund", "Portfolio", "Report_Date"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_country": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "2_gold_aum_kpis_by_reporting_country.py",
        "match_keys": ["Report_Date", "Country"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_lifecycle_phase": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "2_gold_aum_kpis_by_reporting_lifecycle_phase.py",
        "match_keys": ["Report_Date", "Lifecycle_Phase"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_reporting_technology": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "2_gold_aum_kpis_by_reporting_technology.py",
        "match_keys": ["Report_Date", "Technology"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_vs_prev": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "2_gold_aum_kpis_vs_prev.py",
        "match_keys": ["This_Report_Date", "Metric"],
        "load_date_column": "Refresh_Timestamp",
    },

    "aum_kpis_by_fund_vs_prev": {
        "level": 2,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "2_gold_aum_kpis_by_fund_vs_prev.py",
        "match_keys": ["This_Report_Date", "Metric", "Fund"],
        "load_date_column": "Refresh_Timestamp",
    },
}