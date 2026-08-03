table_config = {
      "aum_metrics": {
        "scd2_enabled": False,
        "match_keys": ["ef_portfolio", "ef_fund", "REPORT_DATE"],
        "non_null_columns": [
            "ef_portfolio", "ef_fund", "REPORT_DATE", "ASSET_NAV", "FUND_NAV", "FUM",
            "PORTFOLIO_GAV", "FUND_GAV", "AUM", "DRY_POWDER", "CHECK_ASSET_NAV",
            "CHECK_FUM", "CHECK_AUM", "refresh_timestamp"
        ],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["ef_portfolio", "ef_fund", "REPORT_DATE"]]
        },
    },

    "aum_metrics_enriched": {
        "scd2_enabled": False,
        "match_keys": ["ef_portfolio", "ef_fund", "REPORT_DATE"],
        "non_null_columns": [
            "ef_portfolio", "ef_fund", "REPORT_DATE", "ASSET_NAV", "FUND_NAV", "FUM",
            "PORTFOLIO_GAV", "FUND_GAV", "AUM", "DRY_POWDER", "CHECK_ASSET_NAV",
            "CHECK_FUM", "CHECK_AUM", "refresh_timestamp"
        ],
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
            # "_fund_portfolio_pairs_not_in_cdm": {
            #     "Cdm_Fund_Id": "invp_fund_core_id",
            #     "Cdm_Portfolio_Id": "investment_portfolio_id",
            #     "hard_check": True,
            # }
        },
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["ef_portfolio", "ef_fund", "REPORT_DATE"]]
        },
    },

"aum_capital_injections_and_disposals_transactions": {
            "scd2_enabled": False,
            "match_keys": ["transaction_id"],
            "non_null_columns": ["transaction_id", "cdm_fund_id", "cdm_portfolio_id", "transaction_date", "currency", "tx_bucket_status"],
            "sql_join_checks": {
                  "_portfolio_with_no_mapping_to_cdm": {
                        "ef_portfolio": "source_portfolio_id",
                        "hard_check": True,
                  },
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "individual": ["transaction_id"]
            },
    },
"aggregated_asset_transactions": {
            "scd2_enabled": False,
            "match_keys": ["company", "instrument", "type", "report_date"],
            "non_null_columns": ["company", "instrument", "type", "report_date"],
            "sql_join_checks": {
                  "_portfolio_with_no_mapping_to_cdm": {
                        "investment_portfolio": "source_portfolio_id",
                        "hard_check": True,
                  },
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["company", "company_investor", "investment_portfolio", "instrument", "type", "cashflow_type", "report_date", "instrument_currency"]]
            },
    },
"company":
      {
            "scd2_enabled": False,
            "match_keys":  ["Company_IQId"],
             "non_null_columns":["Company_IQId", "Company", "Created_on", "Modified_on", "refresh_timestamp"],
            "sql_join_checks": {
                  "_company_with_no_mapping_to_cdm": {
                        "Company_IQId": "source_company_id",
                        "hard_check": True,
                  },
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "individual": ["Company_IQId"]
            }
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
            "uniqueness_expectations": {
                  "individual": ["ef_fund_id"]
            }
      },
"fundoperations_raw":
      {
            "scd2_enabled": False,
            "match_keys": ["ef_fund_id", "investor_account", "fund_currency", "effective_date", "amount_fund_curr", "modified_on", "type"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "ef_fund_id": "source_fund_id",
                        "hard_check": True,
                },
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["ef_fund_id", "investor_account", "fund_currency", "effective_date", "amount_fund_curr", "modified_on", "share", "type"]]
            },
            },
"fundoperations_daily":
      {
            "scd2_enabled": False,
            "match_keys": ["Ef_Fund_Id", "Investor", "Transaction_Currency", "Investor_Type", "Investor_Country", "Effective_Date"],
            "non_null_columns": ["Ef_Fund_Id", "Fund", "Investor", "Investor_Type", "Investor_Country", "Effective_Date", "Transaction_Currency", "Commitments", "Called", "Distribution", "NAV", "Cumulative_Commitments", "Cumulative_Called", "Remaining_Commitments", "Cumulative_Distribution", "refresh_timestamp"],
            "sql_join_checks": {
                  "_fund_with_no_mapping_to_cdm": {
                        "Ef_Fund_Id": "source_fund_id",
                        "hard_check": True,
                },
            #       "_investor_in_fact_table_but_not_dim_table": { # fundoperations is fact, investoraccount is dim; investor account is somtimes in left but not in right in eFront cos no enforced key rules
            #             "Investor": "investor_account",
            #             "hard_check": True,
            #     }
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["Ef_Fund_Id", "Investor", "Transaction_Currency", "Investor_Type", "Investor_Country", "Effective_Date"]]
            },
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
            #       "_investor_in_fact_table_but_not_dim_table": {
            #             "Investor": "investor_account",
            #             "hard_check": True,
            #     }
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["Ef_Fund_Id", "Transaction_Currency", "Investor", "Investor_Type", "Year_Month"]]
            },
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
            #       "_investor_in_fact_table_but_not_dim_table": {
            #             "Investor": "investor_account",
            #             "hard_check": True,
            #     }
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["Ef_Fund_Id", "Transaction_Currency", "Investor", "Investor_Type", "Year_Quarter"]]
            },
            },

"fundoperations_by_investor":
      {
            "scd2_enabled": False,
            "match_keys": ["Investor", "Transaction_Currency", "Investor_Type", "Investor_Country", "Effective_Date"],
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
            "uniqueness_expectations": {
                  "composite": [["Ef_Fund_Id", "Investor", "Transaction_Currency", "Investor_Type", "Investor_Country", "Effective_Date"]]
            },
            },


"fundoperations_by_investor_type_monthly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Type", "Year_Month"],
        "non_null_columns": ["Fund", "Investor_Type", "Transaction_Currency", "Year_Month", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["Fund", "Investor_Type", "Transaction_Currency", "Year_Month"]]
        },
    },
"fundoperations_by_investor_country_monthly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Country", "Year_Month"],
        "non_null_columns": ["Fund", "Investor_Country", "Transaction_Currency", "Year_Month", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["Fund", "Investor_Country", "Transaction_Currency", "Year_Month"]]
        },
    },
"fundoperations_by_investor_type_quarterly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Type", "Transaction_Currency", "Year_Quarter", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["Fund", "Investor_Type", "Transaction_Currency", "Year_Quarter"]]
        },
    },
"fundoperations_by_investor_country_quarterly": {
        "scd2_enabled": False,
        "match_keys": ["Fund", "Investor_Country", "Transaction_Currency", "Year_Quarter", "Commitments", "Called", "Distribution", "Cumulative_Commitments", "Cumulative_Called", "Cumulative_Distribution"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["Fund", "Investor_Country", "Transaction_Currency", "Year_Quarter"]]
        },
    },
"fees_company": {
      "scd2_enabled": False,
      "match_keys": ["IQId"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "individual": ["IQId"]
      },
},
"fees_fund": {
      "scd2_enabled": False,
      "match_keys": ["IQId", "Investor_Account_IQId"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "composite": [["IQId", "Investor_Account_IQId"]]
      },
},
"instruments": {
      "scd2_enabled": False,
      "match_keys": ["Company_IQId", "Instrument", "Portfolio", "Description", "Currency"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "composite": [["Company_IQId", "Instrument", "Description", "Portfolio"]]
      },
},
"investor": {
      "scd2_enabled": False,
      "match_keys": ["Investor_IQId"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "individual": ["Investor_IQId"]
      },
},
"ownership": {
      "scd2_enabled": False,
      "match_keys": ["Company_IQId", "Fund_IQId", "Company_Investor_IQId"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "composite": [["Company_IQId", "Fund_IQId", "Company_Investor_IQId"]]
      },
},
"datapointsexport": {
      "scd2_enabled": False,
      "match_keys": ["CATEGORY_NAME", "DATAPOINT_NAME", "ENTITY", "ENTITY_NAME", "REPORTINGDATE"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "composite": [["CATEGORY_NAME", "CREATIONDATE", "DATAPOINT_NAME", "ENTITY", "REFDATE", "row_seq"]]
      },
},
"track_record_fund_raw": {
      "scd2_enabled": False,
      "match_keys": ["FUND_SHORT"],
      "load_date_column": "refresh_timestamp",
      "uniqueness_expectations": {
            "composite": [["FUND_SHORT", "REPORTING_DATE"]]
      },
},
"fx_vs_gbp":
      {
            "scd2_enabled": False,
            "match_keys":  ["Currency", "Ref_Date"],
            "non_null_columns": ["Currency", "Ref_Date", "Rate_to_Gbp"],
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["currency", "ref_date"]]
            },
      },
"investoraccount":
      {
            "scd2_enabled": False,
            "match_keys": ["investor_account_id", "investor_id", "ef_fund_id"],
            "non_null_columns": ["investor_account_id", "investor_id", "ef_fund_id", "investor", "investor_account", "ef_fund", "ef_fund_long"],
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "composite": [["investor_account_id", "investor_id", "ef_fund_id"]]
            },
      },
"investment_portfolio_track_record_reporting": {
        "scd2_enabled": False,
        "match_keys": ["investment_portfolio_id"],
        "non_null_columns": ["fund_id"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
                  "composite": [["investment_portfolio_id"]]
            },
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
            "uniqueness_expectations": {
                  "composite": [["ef_fund", "raw_metric"]]
            },
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
            "uniqueness_expectations": {
                  "composite": [["domain", "source_system_id", "dataset_name", "normalised_metric"]]
            },
      },
"transactions":
      {
            "scd2_enabled": False,
            "match_keys": ["transaction_id"],
            "non_null_columns": ["transaction_id", "effective_date", "type", "description", "investment_portfolio", "company",
            "instrument", "instrument_currency", "investment_details", "transaction_id",
            "company_id",],
            "sql_join_checks": {
                  "_portfolio_with_no_mapping_to_cdm": {
                        "investment_portfolio": "source_portfolio_id",
                        "hard_check": True,
                  },
                  "_company_with_no_mapping_to_cdm": {
                        "company_id": "source_company_id",
                        "hard_check": False,
                  },
            },
            "load_date_column": "refresh_timestamp",
            "uniqueness_expectations": {
                  "individual": ["transaction_id"]
            },
      },
"transactions_paid":
      {
            "scd2_enabled": False,
            "match_keys": ["transaction_id", "date_paid"],
            "non_null_columns": ["transaction_id", "company", "company_id", "investment_portfolio", "type", "description", "instrument", "instrument_currency", "amount_paid", "amount_transaction", "aum_reporting", "effective_date", "date_paid", "created_on", "modified_by", "modified_on", "datalake_ingestion_timestamp"],
            "load_date_column": "refresh_timestamp",
            "sql_join_checks": {
                  "_portfolio_with_no_mapping_to_cdm": {
                        "investment_portfolio": "source_portfolio_id",
                        "hard_check": True,
                  },
                  "_company_with_no_mapping_to_cdm": {
                        "company_id": "source_company_id",
                        "hard_check": False,
                  },
            },
            "uniqueness_expectations": {
                  "composite": [["transaction_id", "date_paid",]]
            }
      },
}