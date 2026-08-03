table_config = {
# fact
"aum_asset":
            {
                "scd2_enabled": False,
                "match_keys": ["COMPANY_NAME", "PORTFOLIO_NAME", "FUND_SHORT", "REPORT_DATE"],
                "load_date_column": "datalake_ingestion_timestamp",
                "non_null_columns": ["COMPANY_NAME", "PORTFOLIO_NAME", "FUND_SHORT", "REPORT_DATE", "AUM", "datalake_ingestion_timestamp"],
                "audit_keys" : [],
                "date_formats": {
                    "REPORT_DATE": "M/d/yyyy h:mm:ss a" # Example format: "2/12/2025 12:00:00 AM"
                    },
                "uniqueness_expectations": {
                    "composite": [["REPORT_DATE", "FUND_SHORT", "PORTFOLIO_NAME", "COMPANY_NAME"]]
                }
            },
# fact
"aum_fund":
            {
                "scd2_enabled": False,
                "match_keys": ["FUND_SHORT", "REPORT_DATE", "FUM", "datalake_ingestion_timestamp"],
                "load_date_column": "datalake_ingestion_timestamp",
                "non_null_columns":["FUND_SHORT"],
                "audit_keys" : [],
                "date_formats": {
                    "REPORT_DATE": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "composite": [["REPORT_DATE", "FUND_SHORT"]]
                }
            },
}