table_config = {
    "company_appointments_fact": {
        "level": 2,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_company_appointments_fact.py",
        "match_keys":  ["gems_company_id", "gems_appointment_type", "gems_appointee_id", "active_from_date"],
        "load_date_column": "source_ingestion_timestamp",
    },

    "company_dim": {
        "level": 1,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_company_dim.py",
        "match_keys":  ["gems_company_id"],
        "load_date_column": "datalake_ingestion_timestamp",
    },

    "company_address_dim": {
        "level": 1,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_company_address_dim.py",
        "match_keys":  ["gems_company_id", "gems_address_id"],
        "load_date_column": "datalake_ingestion_timestamp",
    },

    "company_recon": {
        "level": 2,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_company_recon.py",
        "match_keys": ["reconciled_cdm_id", "gems_company_id"],
        "load_date_column": "refresh_timestamp",
        },

    "shareholdings_fact": {
        "level": 1,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_shareholdings_fact.py",
        "match_keys":  ["company_bridge_parent_id"],
        "load_date_column": "source_ingestion_timestamp",
    },

    "shareholdings_recon": {
        "level": 2,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_shareholdings_recon.py",
        "match_keys":  ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start"],
        "load_date_column": "refresh_timestamp",
    },

    "company_relationships_fact": {
        "level": 2,
        "code_type": "notebook",
        "file_name": "silver_company_relationships_fact",
        "notebook_params": {
            "match_keys": ["rel_id", "transaction_date"],
            "load_date_column": "refresh_timestamp",
            "save_mode": "merge",
            }
    },

    "company_relationships_recon": {
        "level": 3,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_company_relationships_recon.py",
        "match_keys": ["rel_id", "source", "active_from_date"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]]			
        }
    },

    "mandate_to_assetco_relationships_recon": {
        "level": 4,
        "code_type": "sql",
        "table_type": "merge",
        "file_name": "silver_mandate_to_assetco_relationships_recon.py",
        "match_keys": ["rel_id", "source", "active_from_date"],
        "load_date_column": "refresh_timestamp",
        "uniqueness_expectations": {
            "composite": [["rel_id", "source", "active_from_date"]]			
        }
    },
}