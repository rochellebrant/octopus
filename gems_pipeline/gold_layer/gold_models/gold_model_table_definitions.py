gold_table_definitions = {
    "company_reconciliation_with_cdm": {
        "table": {
            "comment": "Silver layer aggregated view/table providing an executive summary of the GEMs to CDM company reconciliation process. Groups data by match status and calculates data quality metrics based on name similarity and incorporation numbers."
        },
        "columns": {
            "reconciliation_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The high-level category of the alignment state (e.g., 'Matched on ID', 'Only in GEMs', 'Only in CDM').",
                    "long_name": "Reconciliation Status",
                    "unit": "n/a"
                }
            },
            "total_companies": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The total count of distinct company records falling into this specific reconciliation status bucket.",
                    "long_name": "Total Companies",
                    "unit": "count"
                }
            },
            "percentage_of_total": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The proportion of total records across all systems that fall into this specific status bucket, rounded to one decimal place.",
                    "long_name": "Percentage of Total (%)",
                    "unit": "%"
                }
            },
            "exact_name_matches": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of records within this bucket where the company names are identical across both systems.",
                    "long_name": "Exact Name Matches",
                    "unit": "count"
                }
            },
            "minor_mismatches": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of records within this bucket where names are not exact matches, but have a Levenshtein similarity score of 85% or higher.",
                    "long_name": "Minor Typos / Fuzzy Matches (>85%)",
                    "unit": "count"
                }
            },
            "major_mismatches": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of records within this bucket where names have a Levenshtein similarity score below 85%, indicating a potential mapping error.",
                    "long_name": "Significant Name Mismatches",
                    "unit": "count"
                }
            },
            "exact_number_matches": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of records within this bucket where the official incorporation numbers perfectly align.",
                    "long_name": "Exact Number Matches",
                    "unit": "count"
                }
            },
            "number_mismatches": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of records within this bucket where the official incorporation numbers conflict between the two systems.",
                    "long_name": "Number Mismatches",
                    "unit": "count"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Gold summary table was last refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "legal_entity_reconciliation_summary": {
        "table": {
            "comment": "Gold layer reporting table summarizing the reconciliation of legal entities (parent and child companies) between the Core Data Model (CDM) and GEMs. Provides high-level KPIs on match rates and system discrepancies for data quality dashboards."
        },
        "columns": {
            "total_unique_entities": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The total count of distinct legal entities found across both the GEMs and CDM systems.",
                    "long_name": "Total Unique Entities",
                    "unit": "count"
                }
            },
            "matching_entities_count": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The total number of legal entities that successfully exist in both GEMs and CDM.",
                    "long_name": "Matching Entities Count",
                    "unit": "count"
                }
            },
            "matching_entities_percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The proportion of total unique entities that are matched across both systems, rounded to 2 decimal places.",
                    "long_name": "Matching Entities Percentage",
                    "unit": "%"
                }
            },
            "gems_only_missing_in_cdm": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of legal entities that exist in the GEMs source system but are missing from the Core Data Model.",
                    "long_name": "GEMs Only (Missing in CDM)",
                    "unit": "count"
                }
            },
            "cdm_only_missing_in_gems": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The count of legal entities that exist in the Core Data Model but are missing from the GEMs source system.",
                    "long_name": "CDM Only (Missing in GEMs)",
                    "unit": "count"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Gold summary table was last refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },
    "legal_entity_reconciliation_exceptions": {
        "table": {
            "comment": "Gold layer exception log detailing the specific legal entities that failed reconciliation between GEMs and CDM. Designed for data stewards and operations teams to identify and remediate missing records."
        },
        "columns": {
            "cdm_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier for the entity.",
                    "long_name": "Entity Internal ID",
                    "unit": "n/a"
                }
            },
            "gems_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific system ID for the entity as recorded in the GEMs source system, if available.",
                    "long_name": "GEMs Entity ID",
                    "unit": "n/a"
                }
            },
            "entity_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The best available name of the legal entity, coalesced from GEMs or CDM records.",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "missing_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Categorical flag indicating which system the entity is missing from (e.g., 'Missing in CDM' or 'Missing in GEMs').",
                    "long_name": "Missing Status",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Gold exceptions table was last refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    }
}