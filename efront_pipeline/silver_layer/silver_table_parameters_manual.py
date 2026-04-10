table_config = {					

"aum_metrics":
      {
            "scd2_enabled": False,
            "match_keys":  ["ef_portfolio", "ef_fund", "REPORT_DATE"],
            "non_null_columns":["ef_portfolio", "ef_fund", "REPORT_DATE", "ASSET_NAV", "FUND_NAV", "FUM", "PORTFOLIO_GAV", "FUND_GAV", "AUM", "DRY_POWDER", "CHECK_ASSET_NAV", "CHECK_FUM", "CHECK_AUM", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
                  "_portfolio_with_no_mapping_to_cdm": {
                        "ef_portfolio_id": "source_portfolio_id",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
      },

"aum_metrics_enriched":
      {
            "scd2_enabled": False,
            "match_keys":  ["ef_portfolio", "ef_fund", "REPORT_DATE"],
            "non_null_columns":["ef_portfolio", "ef_fund", "REPORT_DATE", "ASSET_NAV", "FUND_NAV", "FUM", "PORTFOLIO_GAV", "FUND_GAV", "AUM", "DRY_POWDER", "CHECK_ASSET_NAV", "CHECK_FUM", "CHECK_AUM", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
                  "_portfolio_with_no_mapping_to_cdm": {
                        "ef_portfolio_id": "source_portfolio_id",
                        "hard_check": True,
                },
                  "_rows_with_missing_fields_check": {
                        "ef_fund_id": "ef_fund_id_check",
                        "ef_portfolio_id": "ef_portfolio_id_check",
                        "REPORT_DATE": "REPORT_DATE_check",
                        "hard_check": False,
                  },
            #       "_fund_portfolio_pairs_not_in_cdm": {
            #             "Cdm_Fund_Id": "invp_fund_core_id",
            #             "Cdm_Portfolio_Id": "investment_portfolio_id",
            #             "hard_check": True,
            #     }
            },
            "load_date_column": "refresh_timestamp",
      },
}