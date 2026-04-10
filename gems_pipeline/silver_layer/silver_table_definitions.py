silver_table_definitions = {
    "shareholding_reconciliation": {
        "table": {
            "comment": "Silver layer reconciliation table. Compares contiguous historical time slices of parent-child company ownership percentages between the GEMs API source and the Core Data Model (CDM) to identify variances and missing records."
        },
        "columns": {
            "cdm_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique Core Data Model (CDM) internal identifier for the child company.",
                    "long_name": "CDM Child Company ID",
                    "unit": "n/a"
                }
            },
            "cdm_child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the child company as recorded in the Core Data Model (CDM).",
                    "long_name": "CDM Child Company Name",
                    "unit": "n/a"
                }
            },
            "gems_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific system ID for the child company as recorded in the GEMs source system.",
                    "long_name": "GEMs Child Company ID",
                    "unit": "n/a"
                }
            },
            "gems_child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the child company as recorded in the GEMs source system.",
                    "long_name": "GEMs Child Company Name",
                    "unit": "n/a"
                }
            },
            "cdm_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique Core Data Model (CDM) internal identifier for the parent company.",
                    "long_name": "CDM Parent Company ID",
                    "unit": "n/a"
                }
            },
            "cdm_parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the parent company as recorded in the Core Data Model (CDM).",
                    "long_name": "CDM Parent Company Name",
                    "unit": "n/a"
                }
            },
            "gems_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific system ID for the parent company as recorded in the GEMs source system.",
                    "long_name": "GEMs Parent Company ID",
                    "unit": "n/a"
                }
            },
            "gems_parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the parent company as recorded in the GEMs source system.",
                    "long_name": "GEMs Parent Company Name",
                    "unit": "n/a"
                }
            },
            "comparison_slice_start": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The start date of the contiguous time slice being evaluated.",
                    "long_name": "Time Slice Start Date",
                    "unit": "n/a"
                }
            },
            "comparison_slice_end": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The end date of the contiguous time slice being evaluated. A null value indicates an ongoing/current active relationship.",
                    "long_name": "Time Slice End Date",
                    "unit": "n/a"
                }
            },
            "cdm_ownership_percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The parent's ownership percentage of the child company during this time slice, as stated in the CDM.",
                    "long_name": "CDM Ownership Percentage",
                    "unit": "%"
                }
            },
            "gems_ownership_percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The parent's ownership percentage of the child company during this time slice, as stated in GEMs.",
                    "long_name": "GEMs Ownership Percentage",
                    "unit": "%"
                }
            },
            "percentage_variance": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The delta between the GEMs ownership percentage and the CDM ownership percentage (GEMs minus CDM).",
                    "long_name": "Ownership Percentage Variance",
                    "unit": "%"
                }
            },
            "match_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Categorical flag indicating the alignment state between GEMs and CDM for this specific time slice.",
                    "long_name": "Reconciliation Match Status",
                    "unit": "n/a"
                }
            },
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique Core Data Model (CDM) identifier for the associated fund.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary display name of the associated fund sourced from the CDM.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique Core Data Model (CDM) identifier for the investment portfolio.",
                    "long_name": "CDM Investment Portfolio ID",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investment portfolio the child company belongs to.",
                    "long_name": "CDM Investment Portfolio Name",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "gems_cdm_company_reconciliation": {
        "table": {
            "comment": "Silver layer reconciliation table. Performs a full outer join between CDM and GEMs company dimensions to identify missing records, exact matches, and calculates Levenshtein-based fuzzy match similarity scores for company names."
        },
        "columns": {
            "reconciled_cdm_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The resolved Core Data Model (CDM) internal identifier, coalesced from either the CDM source or the GEMs mapping.",
                    "long_name": "Reconciled CDM ID",
                    "unit": "n/a"
                }
            },
            "gems_unique_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier for the company as recorded in the GEMs source system.",
                    "long_name": "GEMs Unique ID",
                    "unit": "n/a"
                }
            },
            "id_match_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Categorical flag indicating whether the company exists in both systems, only in CDM, or only in GEMs based on the ID mapping.",
                    "long_name": "ID Match Status",
                    "unit": "n/a"
                }
            },
            "cdm_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered company name sourced from the Core Data Model (CDM).",
                    "long_name": "CDM Company Name",
                    "unit": "n/a"
                }
            },
            "gems_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The company name sourced from the GEMs system.",
                    "long_name": "GEMs Company Name",
                    "unit": "n/a"
                }
            },
            "is_name_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating if the trimmed, lowercased names perfectly match between CDM and GEMs.",
                    "long_name": "Is Name Exact Match",
                    "unit": "boolean"
                }
            },
            "name_similarity_score": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "A normalized score from 0.0 to 1.0 representing the Levenshtein distance between the CDM and GEMs company names. 1.0 represents a perfect match.",
                    "long_name": "Name Similarity Score",
                    "unit": "decimal"
                }
            },
            "cdm_incorporation_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The official incorporation number for the company as recorded in the CDM.",
                    "long_name": "CDM Incorporation Number",
                    "unit": "n/a"
                }
            },
            "gems_company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The official company number as recorded in the GEMs system.",
                    "long_name": "GEMs Company Number",
                    "unit": "n/a"
                }
            },
            "is_number_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating if the trimmed incorporation numbers perfectly match between the two systems.",
                    "long_name": "Is Number Exact Match",
                    "unit": "boolean"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver fact table record was computed and refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company_fact_parents": {
        "table": {
            "comment": "Silver layer fact table recording the historical, contiguous time-sliced ownership percentages between parent and child companies. Sourced from GEMs daily transactions and mapped to CDM internal identifiers."
        },
        "columns": {
            "company_bridge_parent_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A unique, deterministic SHA-256 hash generated from the child ID, parent ID, and active from date. Used as a primary key for incremental DLT loading.",
                    "long_name": "Company Bridge Parent Hash ID",
                    "unit": "n/a"
                }
            },
            "company_core_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the child company.",
                    "long_name": "Child Company Core ID",
                    "unit": "n/a"
                }
            },
            "parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the parent/holding company.",
                    "long_name": "Parent Company Core ID",
                    "unit": "n/a"
                }
            },
            "ownership_percentage": {
                "schema": {"data_type": "FLOAT"},
                "description": {
                    "comment": "The calculated percentage of the child company owned by the parent during this specific time slice.",
                    "long_name": "Ownership Percentage",
                    "unit": "%"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The exact timestamp when this specific ownership percentage slice became active, derived from the transaction date.",
                    "long_name": "Active From Timestamp",
                    "unit": "n/a"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The timestamp when this specific ownership percentage slice ended. A null value indicates this is the currently active, ongoing ownership state.",
                    "long_name": "Active To Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver fact table record was computed and refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company_parent_relationship_map": {
        "table": {
            "comment": "Silver layer graph engine output. Calculates the multi-tier, time-versioned effective ownership paths between ultimate parent companies (e.g., funds) and ultimate child companies (e.g., assets) by traversing chronological direct ownership edges."
        },
        "columns": {
            "rel_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique identifier for the computed ownership path, constructed by concatenating the individual edge IDs along the traversal path (e.g., 'A.B_B.C').",
                    "long_name": "Relationship Path ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the entity at the very top of this specific ownership path.",
                    "long_name": "Ultimate Parent ID",
                    "unit": "n/a"
                }
            },
            "ultimate_child_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the target asset/company at the very bottom of this specific ownership path.",
                    "long_name": "Ultimate Child ID",
                    "unit": "n/a"
                }
            },
            "percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The effective, multiplied ownership percentage of the ultimate child by the ultimate parent along this specific path, expressed as a decimal between 0.0 and 1.0.",
                    "long_name": "Effective Path Percentage",
                    "unit": "decimal"
                }
            },
            "transaction_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The effective date when this specific relationship path and percentage became active, driven by the underlying transaction events.",
                    "long_name": "Effective Transaction Date",
                    "unit": "n/a"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A comma-separated string of the distinct transaction types (e.g., 'Passive Dilution/Accretion', 'Partnership Appointment - General Partner') associated with the underlying edges that form this specific relationship path.",
                    "long_name": "Path Transaction Types",
                    "unit": "n/a"
                }
            },
            "relationship_version": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The chronological version number of this specific relationship path. Version 1 represents the earliest appearance of this state.",
                    "long_name": "Relationship Version Number",
                    "unit": "count"
                }
            },
            "relationship_level": {
                "schema": {"data_type": "BIGINT"},
                "description": {
                    "comment": "The depth or number of hops (edges) in this specific ownership path. A value of 1 indicates direct ownership.",
                    "long_name": "Relationship Path Depth",
                    "unit": "count"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this multi-tier relationship path was computed and refreshed by the graph engine.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "gems_cdm_fund_to_assetco_relationship_map_reconciliation": {
        "table": {
            "comment": "Silver layer table that unions GEMs and CDM parent-child relationships, filtered specifically for funds to asset companies. Applies window functions to generate continuous historical ownership timelines (active from/to dates) per source system."
        },
        "columns": {
            "rel_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique identifier for the specific Fund-to-AssetCo relationship record, passed through from the underlying source system.",
                    "long_name": "Relationship ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Core Data Model (CDM) internal identifier for the ultimate parent company (the fund).",
                    "long_name": "Ultimate Parent ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate parent company (the fund), sourced from the Core Data Model (CDM).",
                    "long_name": "Ultimate Parent Name",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Core Data Model (CDM) internal identifier for the ultimate child company (the SPV).",
                    "long_name": "Ultimate Child ID",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate child company (SPV), sourced from the Core Data Model (CDM).",
                    "long_name": "Ultimate Child Name",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The origin system of this relationship timeline slice (e.g., 'gems' or 'cdm').",
                    "long_name": "Source System",
                    "unit": "n/a"
                }
            },
            "percentage_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of the child company owned by the parent fund during this specific time slice.",
                    "long_name": "Percentage Ownership",
                    "unit": "%"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A comma-separated string of the distinct transaction types (e.g., 'Passive Dilution/Accretion', 'Partnership Appointment - General Partner') associated with the underlying edges that form this specific relationship path.",
                    "long_name": "Path Transaction Types",
                    "unit": "n/a"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The start date of this specific ownership percentage slice, derived from the transaction date.",
                    "long_name": "Active From Date",
                    "unit": "n/a"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The end date of this specific ownership percentage slice. A null value indicates this is the currently active, ongoing ownership state.",
                    "long_name": "Active To Date",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of Core Data Model (CDM) investment portfolio IDs mapped to the ultimate child company.",
                    "long_name": "CDM Investment Portfolio IDs Array",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of investment portfolio names mapped to the ultimate child company.",
                    "long_name": "CDM Investment Portfolio Names Array",
                    "unit": "n/a"
                }
            },
            "cdm_fund_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of Core Data Model (CDM) fund IDs mapped to the ultimate child company.",
                    "long_name": "CDM Fund Core IDs Array",
                    "unit": "n/a"
                }
            },
            "cdm_fund_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of fund display names mapped to the ultimate child company.",
                    "long_name": "CDM Fund Display Names Array",
                    "unit": "n/a"
                }
            },
            "company_path_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "An ordered, deduplicated array of registered company names representing the ownership hierarchy path from ultimate parent down to ultimate child.",
                    "long_name": "Company Path Names Array",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "gems_cdm_relationship_map_reconciliation": {
        "table": {
            "comment": "Silver layer table that unions GEMs and CDM parent-child relationships. Applies window functions to generate continuous historical ownership timelines (active from/to dates) per source system, and enriches with associated funds, portfolios, and the ordered ownership hierarchy path."
        },
        "columns": {
            "rel_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique identifier for the specific relationship record, representing the hierarchy path (e.g., 'COMP_1.COMP_2'). Passed through from the underlying source system.",
                    "long_name": "Relationship ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Core Data Model (CDM) internal identifier for the ultimate parent company at the top of the relationship path.",
                    "long_name": "Ultimate Parent ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate parent company, sourced from the Core Data Model (CDM).",
                    "long_name": "Ultimate Parent Name",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Core Data Model (CDM) internal identifier for the ultimate child company at the bottom of the relationship path.",
                    "long_name": "Ultimate Child ID",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate child company, sourced from the Core Data Model (CDM).",
                    "long_name": "Ultimate Child Name",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The origin system of this relationship timeline slice (e.g., 'GEMs' or 'CDM').",
                    "long_name": "Source System",
                    "unit": "n/a"
                }
            },
            "percentage_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of the child company owned by the parent entity during this specific time slice.",
                    "long_name": "Percentage Ownership",
                    "unit": "%"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A comma-separated string of the distinct transaction types (e.g., 'Passive Dilution/Accretion', 'Partnership Appointment - General Partner') associated with the underlying edges that form this specific relationship path.",
                    "long_name": "Path Transaction Types",
                    "unit": "n/a"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The start date of this specific ownership percentage slice, derived from the transaction date.",
                    "long_name": "Active From Date",
                    "unit": "n/a"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The end date of this specific ownership percentage slice. A null value indicates this is the currently active, ongoing ownership state.",
                    "long_name": "Active To Date",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of Core Data Model (CDM) investment portfolio IDs mapped to the ultimate child company.",
                    "long_name": "CDM Investment Portfolio IDs Array",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of investment portfolio names mapped to the ultimate child company.",
                    "long_name": "CDM Investment Portfolio Names Array",
                    "unit": "n/a"
                }
            },
            "cdm_fund_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of Core Data Model (CDM) fund IDs mapped to the ultimate child company.",
                    "long_name": "CDM Fund Core IDs Array",
                    "unit": "n/a"
                }
            },
            "cdm_fund_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "A deduplicated array of fund display names mapped to the ultimate child company.",
                    "long_name": "CDM Fund Display Names Array",
                    "unit": "n/a"
                }
            },
            "company_path_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "An ordered, deduplicated array of registered company names representing the ownership hierarchy path from ultimate parent down to ultimate child.",
                    "long_name": "Company Path Names Array",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    }
}