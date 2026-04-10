table_config = {
    "company_reconciliation_with_cdm": {
        "level": 1,
        "code_type": "sql",
        "table_type": "replace",
        "file_name": "1_gold_company_reconciliation_with_cdm.py",
        "match_keys": ["reconciliation_status", "total_companies"],
        "load_date_column": "refresh_timestamp",
    },

    # "legal_entity_reconciliation_summary": {
    #     "level": 1,
    #     "code_type": "sql",
    #     "table_type": "replace",
    #     "file_name": "1_gold_legal_entity_reconciliation_summary.py",
    #     "match_keys": ["matching_entities_count", "matching_entities_percentage", "gems_only_missing_in_cdm", "cdm_only_missing_in_gems"],
    #     "load_date_column": "refresh_timestamp",
    # },

    # "legal_entity_reconciliation_exceptions": {
    #     "level": 1,
    #     "code_type": "sql",
    #     "table_type": "replace",
    #     "file_name": "1_gold_legal_entity_reconciliation_exceptions.py",
    #     "match_keys": ["internal_id", "gems_id", "missing_status"],
    #     "load_date_column": "refresh_timestamp",
    # },
}