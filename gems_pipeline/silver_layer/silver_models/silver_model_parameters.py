table_config = {
    "shareholdings": {
        "level": 1,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_shareholdings.py",
        "match_keys":  ["company_bridge_parent_id"],
        "load_date_column": "refresh_timestamp",
    },

    "company_reconciliation_with_cdm": {
        "level": 1,
        "code_type": "sql",
        "file_name": "silver_company_reconciliation_with_cdm.py",
        "match_keys": ["reconciled_cdm_id", "gems_unique_id"],
        "load_date_column": "refresh_timestamp",
        },

    "shareholdings_reconciliation_with_cdm": {
        "level": 2,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_shareholdings_reconciliation_with_cdm.py",
        "match_keys":  ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start"],
        "load_date_column": "refresh_timestamp",
    },

    "company_parent_relationship_map": {
        "level": 2,
        "code_type": "notebook",
        "file_name": "silver_company_parent_relationship_map",
        "notebook_params": {
            "match_keys": ["rel_id", "transaction_date"],
            "load_date_column": "refresh_timestamp",
            "save_mode": "overwrite",
            }
    },

    "relationship_map_reconciliation_with_cdm": {
        "level": 3,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_relationship_map_reconciliation_with_cdm.py",
        "match_keys": ["rel_id", "source", "active_from_date"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]]			
        }
    },

    "fund_to_assetco_relationship_map_reconciliation_with_cdm": {
        "level": 4,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_fund_to_assetco_relationship_map_reconciliation_with_cdm.py",
        "match_keys": ["rel_id", "source", "active_from_date"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]]			
        }
    },
}