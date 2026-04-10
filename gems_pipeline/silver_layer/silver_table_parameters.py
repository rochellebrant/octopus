table_config = {
    "shareholdings_reconciliation_with_cdm": {
        "scd2_enabled": True,
        "match_keys":  ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start"],
        "non_null_columns": ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start", "match_status"],
        "uniqueness_expectations": {
            "composite": [["cdm_child_company_id", "cdm_child_parent_id", "comparison_slice_start"]]			
            },
        "load_date_column": "refresh_timestamp",
        },

    "shareholdings": {
        "scd2_enabled": True,
        "match_keys": ["company_bridge_parent_id"],
        "non_null_columns": ["company_bridge_parent_id", "company_core_id", "parent_company_id", "ownership_percentage", "active_from_date"],
        "uniqueness_expectations": {
            "composite": [["company_core_id", "parent_company_id", "ownership_percentage", "active_from_date"]],
            "individual": ["company_bridge_parent_id"]			
        },
        "load_date_column": "refresh_timestamp",
        },
    
    "company_reconciliation_with_cdm": {
        "scd2_enabled": True,
        "match_keys": ["reconciled_cdm_id", "gems_unique_id"],
        "exempt_from_null_checks": ["reconciled_cdm_id", "gems_unique_id"],
        "non_null_columns": ["id_match_status", "is_name_exact_match", "name_similarity_score", "is_number_exact_match"],
        "uniqueness_expectations": {
            "composite": [["reconciled_cdm_id", "gems_unique_id"]],
        },
        "load_date_column": "refresh_timestamp",
        },
    
    "company_parent_relationship_map": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "transaction_date"],
        "non_null_columns": ["rel_id", "percentage", "transaction_date", "relationship_version", "ultimate_child_id", "ultimate_parent_id", "relationship_level"],
        "uniqueness_expectations": {
            "composite": [["rel_id", "transaction_date"]],
        },
        "load_date_column": "refresh_timestamp",
        },
    
    "relationship_map_reconciliation_with_cdm": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "source", "active_from_date"],
        "non_null_columns": ["rel_id", "ultimate_parent_company_id", "ultimate_child_company_id", "source", "percentage_ownership", "active_from_date"],
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]],
        },
        "load_date_column": "refresh_timestamp",
    },
    
    "fund_to_assetco_relationship_map_reconciliation_with_cdm": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "source", "active_from_date"],
        "non_null_columns": ["rel_id", "ultimate_parent_company_id", "ultimate_child_company_id", "source", "percentage_ownership", "active_from_date"],
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]],
        },
        "load_date_column": "refresh_timestamp",
    },

    
# "aum_metrics_enriched":
#       {
#             "scd2_enabled": False,
#             "match_keys":  ["ef_portfolio", "ef_fund", "REPORT_DATE"],
#             "non_null_columns":["ef_portfolio", "ef_fund", "REPORT_DATE", "ASSET_NAV", "FUND_NAV", "FUM", "PORTFOLIO_GAV", "FUND_GAV", "AUM", "DRY_POWDER", "CHECK_ASSET_NAV", "CHECK_FUM", "CHECK_AUM", "refresh_timestamp"],
#             "sql_join_checks": {
#                   "_fund_with_no_mapping_to_cdm": {
#                         "ef_fund_id": "source_fund_id",
#                         "hard_check": True,
#                 },
#                   "_portfolio_with_no_mapping_to_cdm": {
#                         "ef_portfolio_id": "source_portfolio_id",
#                         "hard_check": True,
#                 },
#                   "_rows_with_missing_fields_check": {
#                         "ef_fund_id": "ef_fund_id_check",
#                         "ef_portfolio_id": "ef_portfolio_id_check",
#                         "REPORT_DATE": "REPORT_DATE_check",
#                         "hard_check": False,
#                   },
#             #       "_fund_portfolio_pairs_not_in_cdm": {
#             #             "Cdm_Fund_Id": "invp_fund_core_id",
#             #             "Cdm_Portfolio_Id": "investment_portfolio_id",
#             #             "hard_check": True,
#             #     }
#             },
#             "load_date_column": "refresh_timestamp",
#       },
}