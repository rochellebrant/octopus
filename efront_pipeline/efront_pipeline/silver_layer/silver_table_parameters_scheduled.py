table_config = {
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
                        "Investor": "Investor_Account",
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
                        "Investor": "Investor_Account",
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
                        "Investor": "Investor_Account",
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
                        "Investor": "Investor_Account",
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
}