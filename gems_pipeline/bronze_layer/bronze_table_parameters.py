table_config = {
    "api_appointment": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp", 
        "apply_as_deletes_expr": "is_deleted = True",
        # Composite key: A specific person holding a specific role at a specific company
        "match_keys": ["company_id", "appointee_entity_id", "appointment_type"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "composite": [["company_id", "appointee_entity_id", "appointment_type"]]
        },
        "non_null_columns": ["company_id", "appointee_entity_id", "date_appointed"],
    },
    "api_person": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp",
        "apply_as_deletes_expr": "is_deleted = True", 
        "match_keys": ["entity_id"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "individual": ["entity_id"]
        },
        "non_null_columns": ["entity_id", "name"],
    },
    "api_people": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp", 
        "apply_as_deletes_expr": "is_deleted = True",
        "match_keys": ["person_id"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "individual": ["person_id"]
        },
        "non_null_columns": ["person_id", "full_name"] 
    },
    "api_company_address": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp",
        "apply_as_deletes_expr": "is_deleted = True",
        # A specific company might have multiple addresses, but only one of each type (e.g. one Registered Office). 
        # Adding address_id ensures absolute uniqueness.
        "match_keys": ["company_id", "address_id", "address_type"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "composite": [["company_id", "address_id", "address_type"]]
        },
        "non_null_columns": ["company_id", "address_id"] 
    },
    "api_entity": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp", 
        "apply_as_deletes_expr": "is_deleted = True",
        "match_keys": ["company_id"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "individual": ["company_id"]
        },
        "non_null_columns": ["company_id", "name"],
    },
    "api_member_guarantees": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp", 
        "apply_as_deletes_expr": "is_deleted = True",
        "match_keys": ["company_id", "member_name", "membership_type"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "composite": [["company_id", "member_name", "membership_type"]]
        },
        "non_null_columns": ["company_id", "member_name"],
        "date_formats": {
            "date_appointed": "yyyy-MM-dd'T'HH:mm:ss",
            "date_resigned": "yyyy-MM-dd'T'HH:mm:ss"
        },
    },
    "api_shareholdings": {
        "scd2_enabled": True,
        "load_date_column": "datalake_ingestion_timestamp", 
        "apply_as_deletes_expr": "is_deleted = True",
        "match_keys": ["transaction_id"], 
        "audit_keys": ["table_id", "row_hash"], 
        "uniqueness_expectations": {
            "individual": ["transaction_id"]
        },
        "non_null_columns": ["company_id", "transaction_id"],
        "date_formats": {
            "date_of_transaction": "yyyy-MM-dd'T'HH:mm:ss"
        },
    },
}