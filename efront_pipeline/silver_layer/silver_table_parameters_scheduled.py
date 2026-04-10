table_config = {
# "aum_capital_injections_and_disposals_transactions":
#       {
#             "scd2_enabled": False,
#             "match_keys":  ["transaction_id"],
#             "non_null_columns": ["transaction_id", "cdm_fund_id", "cdm_portfolio_id", "ef_fund_id", "ef_portfolio_id", "transaction_id", "ef_fund", "ef_portfolio", "investor", "type", "amount", "transaction_date", "currency", "transaction_month_end", "used_rate_to_gbp", "amount_gbp" ,"refresh_timestamp"],
#             "sql_join_checks": {
#                   "_fund_with_no_mapping_to_cdm": {
#                         "ef_fund_id": "source_fund_id",
#                         "hard_check": True,
#                 },
#                   "_portfolio_with_no_mapping_to_cdm": {
#                         "ef_portfolio_id": "source_portfolio_id",
#                         "hard_check": True,
#                 },
#             },
#             "load_date_column": "refresh_timestamp",
#       },

"company":
      {
            "scd2_enabled": False,
            "match_keys":  ["Company_IQId"],
            "non_null_columns":["Company_IQId", "Company", "Created_on", "Modified_on", "refresh_timestamp"],
            "sql_join_checks": {
                  "_company_with_no_mapping_to_cdm": {
                        "Company_IQId": "source_company_id",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
      },

"fund":
      {
            "scd2_enabled": False,
            "match_keys": ["ef_fund_id"],
            "non_null_columns": ["cdm_fund_id",	"cdm_fund",	"ef_fund_id", "ef_fund_long", "ef_fund",],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
            },

"fundoperations_daily":
      {
            "scd2_enabled": False,
            "match_keys": ["Ef_Fund_Id", "Investor", "Transaction_Currency", "Investor_Type", "Effective_Date"],
            "non_null_columns": ["Ef_Fund_Id", "Fund", "Investor", "Investor_Type", "Investor_Country", "Effective_Date", "Transaction_Currency", "Commitments", "Called", "Distribution", "NAV", "Cumulative_Commitments", "Cumulative_Called", "Remaining_Commitments", "Cumulative_Distribution", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "Ef_Fund_Id": "source_fund_id",
                        "hard_check": True,
                },
                  "_investor_in_fact_table_but_not_dim_table": { # fundoperations is fact, investoraccount is dim; investor account is somtimes in left but not in right in eFront cos no enforced key rules
                        "Investor": "investor_account",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
            },

"fundoperations_monthly":
      {
            "scd2_enabled": False,
            "match_keys": ["Ef_Fund_Id", "Transaction_Currency", "Investor", "Investor_Type", "Year_Month"],
            "non_null_columns": ["Ef_Fund_Id", "Fund", "Investor", "Investor_Type", "Investor_Country", "Year_Month", "Transaction_Currency", "Commitments", "Called", "Distribution", "NAV", "Cumulative_Commitments", "Cumulative_Called", "Remaining_Commitments", "Cumulative_Distribution", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "Ef_Fund_Id": "source_fund_id",
                        "hard_check": True,
                },
                  "_investor_in_fact_table_but_not_dim_table": {
                        "Investor": "investor_account",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
            },
      
"fundoperations_quarterly":
      {
            "scd2_enabled": False,
            "match_keys": ["Ef_Fund_Id", "Transaction_Currency", "Investor", "Investor_Type", "Year_Quarter"],
            "non_null_columns": ["Ef_Fund_Id", "Fund", "Investor", "Investor_Type", "Investor_Country", "Year_Quarter", "Transaction_Currency", "Commitments", "Called", "Distribution", "NAV", "Cumulative_Commitments", "Cumulative_Called", "Remaining_Commitments", "Cumulative_Distribution", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "Ef_Fund_Id": "source_fund_id",
                        "hard_check": True,
                },
                  "_investor_in_fact_table_but_not_dim_table": {
                        "Investor": "investor_account",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
            },

"fundoperations_by_investor":
      {
            "scd2_enabled": False,
            "match_keys": ["Investor", "Transaction_Currency", "Investor_Type", "Effective_Date"],
            "non_null_columns": ["Ef_Fund_Id", "Fund", "Investor", "Investor_Type", "Investor_Country", "Effective_Date", "Transaction_Currency", "Commitments", "Called", "Distribution", "NAV", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution", "Remaining_Commitments", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "Ef_Fund_Id": "source_fund_id",
                        "hard_check": True,
                },
                  "_investor_in_fact_table_but_not_dim_table": {
                        "Investor": "investor_account",
                        "hard_check": True,
                }
            },
            "load_date_column": "refresh_timestamp",
            },
      
 
"fundoperations_by_investor_type_monthly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Type", "Year_Month"],
        "non_null_columns": ["Fund", "Investor_Type", "Transaction_Currency", "Year_Month", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
    },
"fundoperations_by_investor_country_monthly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Country", "Year_Month"],
        "non_null_columns": ["Fund", "Investor_Country", "Transaction_Currency", "Year_Month", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
    },
"fundoperations_by_investor_type_quarterly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Type", "Transaction_Currency", "Year_Quarter", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
    },
"fundoperations_by_investor_country_quarterly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Country", "Transaction_Currency", "Year_Quarter", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
    },
"fx_vs_gbp":
      {
            "scd2_enabled": False,
            "match_keys":  ["Currency", "Ref_Date"],
            "non_null_columns": ["Currency", "Ref_Date", "Rate_to_Gbp"],
            "load_date_column": "refresh_timestamp",
      },
"investoraccount":
      {
            "scd2_enabled": False,
            "match_keys": ["investor_account_id", "investor_id", "ef_fund_id"],
            "non_null_columns": ["investor_account_id", "investor_id", "ef_fund_id", "investor", "investor_account", "ef_fund", "ef_fund_long"],
            "load_date_column": "refresh_timestamp",
      },
"track_record_fund":
      {
            "scd2_enabled": False,
            "match_keys":  ["ef_fund", "raw_metric"],
            "non_null_columns": ["ef_fund", "ef_fund_id", "cdm_fund", "cdm_fund_id", "fund_is_active", "inception_date", "raw_metric", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
      },
"track_record_metric_dictionary":
      {
            "scd2_enabled": False,
            "match_keys":  ["domain", "source_system_id", "dataset_name", "normalised_metric"],
            "non_null_columns": ["domain", "source_system_id", "dataset_name", "normalised_metric", "metric", "definition"],
            "sql_join_checks": {
                  "_source_system_id_with_no_mapping_to_cdm": {
                        "source_system_id": "source_system_id",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
      },
"transactions":
      {
            "scd2_enabled": False,
            "match_keys": ["transaction_id"],
            "non_null_columns": ["effective_date", "type", "description", "portfolio", "company", "company_investor",
            "complete_name", "instrument", "instrument_currency", "investment_details", "transaction_id",
            "company_id", "company_investor_id"],
            "sql_join_checks": {
                  "_portfolio_with_no_mapping_to_cdm": {
                        "portfolio": "source_portfolio_name",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
      },
}