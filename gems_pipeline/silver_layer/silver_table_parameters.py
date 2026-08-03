table_config = {
    "company_appointments_fact": {
        "scd2_enabled": True,
        "match_keys": ["gems_company_id", "gems_appointment_type", "gems_appointee_id", "active_from_date"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["gems_company_id", "gems_appointment_type", "gems_appointee_id", "active_from_date"],
        "sql_join_checks": {
                  "_company_with_no_cdm_id": {
                        "cdm_company_id": "company_core_id",
                        "hard_check": False,
                  }
                },
        "load_date_column": "source_ingestion_timestamp",
    },

    "company_dim": {
        "scd2_enabled": True,
        "match_keys": ["gems_company_id"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["gems_company_id"],
        "load_date_column": "datalake_ingestion_timestamp",
        },
    
    "company_address_dim": {
        "scd2_enabled": True,
        "match_keys": ["gems_company_id", "gems_address_id"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["gems_company_id", "gems_address_id"],
        "load_date_column": "datalake_ingestion_timestamp",
        },

    "shareholdings_fact": {
        "scd2_enabled": True,
        "match_keys": ["company_bridge_parent_id"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["company_bridge_parent_id", "child_company_id", "parent_company_id", "ownership_percentage", "transaction_type", "active_from_date"],
        "load_date_column": "source_ingestion_timestamp",
        },

    "shareholdings_recon": {
        "scd2_enabled": True,
        "match_keys":  ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["cdm_child_company_id", "cdm_parent_company_id", "comparison_slice_start", "match_status"],
        "load_date_column": "refresh_timestamp",
        },
    
    "company_recon": {
        "scd2_enabled": True,
        "match_keys": ["reconciled_cdm_id", "gems_company_id"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "exempt_from_null_checks": ["reconciled_cdm_id", "gems_company_id"],
        "non_null_columns": ["id_match_status", "is_name_exact_match", "name_similarity_score", "is_number_exact_match"],
        "load_date_column": "refresh_timestamp",
        },
    
    "company_relationships_fact": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "transaction_date"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["rel_id", "percentage", "transaction_date", "relationship_version", "ultimate_child_id", "ultimate_parent_id", "relationship_level"],
        "load_date_column": "refresh_timestamp",
        },
    
    "company_relationships_recon": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "source", "active_from_date"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["rel_id", "ultimate_parent_company_id", "ultimate_child_company_id", "source", "percentage_ownership", "active_from_date"],
        "load_date_column": "refresh_timestamp",
    },
    
    "mandate_to_assetco_relationships_recon": {
        "scd2_enabled": True,
        "match_keys": ["rel_id", "source", "active_from_date"],
        "apply_as_deletes_expr": "is_deleted = true",
        "audit_keys": ["refresh_timestamp"],
        "non_null_columns": ["rel_id", "ultimate_parent_company_id", "ultimate_child_company_id", "source", "percentage_ownership", "active_from_date"],
        "load_date_column": "refresh_timestamp",
    },
}