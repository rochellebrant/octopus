table_config = {
    "aum_metrics": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "silver_aum_metrics.py",
        "match_keys": ["ef_portfolio", "ef_fund", "REPORT_DATE"],
        "load_date_column": "refresh_timestamp",
    },
    "aum_metrics_enriched": {
        "level": 2,
        "code_type": "notebook",
        "file_name": "silver_aum_metrics_enriched",
        "notebook_params": {
            "match_keys": ["ef_portfolio", "ef_fund", "REPORT_DATE"],
            "load_date_column": "refresh_timestamp",
            "save_mode": "overwrite",
            # "period_column": "REPORT_DATE"
            }
    },
}