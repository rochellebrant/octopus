silver_table_definitions = {
    "company_appointments_fact": {
        "table": {
            "comment": "Silver layer fact table containing company appointments. Enriches bronze appointment data with appointee person details and company dimension metadata, and derives appointment active status flags from appointment and resignation dates."
        },
        "columns": {
            "cdm_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal company identifier, sourced from api_appointment.company_internal_id.",
                    "long_name": "Company CDM ID",
                    "unit": "n/a"
                }
            },
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "GEMs company identifier, sourced from api_appointment.company_id.",
                    "long_name": "GEMs Company ID",
                    "unit": "n/a"
                }
            },
            "gems_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company name recorded on the appointment record, sourced from api_appointment.company_name.",
                    "long_name": "GEMs Company Name",
                    "unit": "n/a"
                }
            },
            "gems_company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Official company registration number, sourced from company_dim.gems_company_number.",
                    "long_name": "GEMs Company Number",
                    "unit": "n/a"
                }
            },
            "gems_entity_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the external, legal corporate structure/form of the company (e.g., LLC, GmbH, S.A.).",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "gems_company_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the internal, commercial function or structural role the company plays within Octopus' corporate hierarchy (e.g., HoldCo, SPV, Fund).",
                    "long_name": "Company Type",
                    "unit": "n/a"
                }
            },
            "gems_jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Jurisdiction where the company is registered, sourced from company_dim.gems_jurisdiction.",
                    "long_name": "GEMs Jurisdiction",
                    "unit": "n/a"
                }
            },
            "gems_appointment_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Appointment role or type recorded in GEMs, sourced from api_appointment.appointment_type.",
                    "long_name": "GEMs Appointment Type",
                    "unit": "n/a"
                }
            },
            "cdm_appointee_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal appointee identifier, sourced from api_appointment.appointee_internal_id.",
                    "long_name": "Appointee CDM ID",
                    "unit": "n/a"
                }
            },
            "gems_appointee_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "GEMs appointee entity identifier, sourced from api_appointment.appointee_entity_id.",
                    "long_name": "GEMs Appointee ID",
                    "unit": "n/a"
                }
            },
            "gems_appointee_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Full name of the appointed person, sourced from api_person.name.",
                    "long_name": "GEMs Appointee Name",
                    "unit": "n/a"
                }
            },
            "gender": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Gender of the appointee, sourced from api_person.gender.",
                    "long_name": "Appointee Gender",
                    "unit": "n/a"
                }
            },
            "nationality": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Nationality of the appointee, sourced from api_person.nationality.",
                    "long_name": "Appointee Nationality",
                    "unit": "n/a"
                }
            },
            "is_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Derived flag indicating whether the appointment is active as of the current date. False when the current date is before date_appointed or after date_resigned; otherwise true.",
                    "long_name": "Is Active",
                    "unit": "boolean"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Appointment start date, derived by casting api_appointment.date_appointed to DATE.",
                    "long_name": "Active From Date",
                    "unit": "n/a"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Appointment end date, derived by casting api_appointment.date_resigned to DATE.",
                    "long_name": "Active To Date",
                    "unit": "n/a"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Resolved resignation date. Values of '-' or NULL in api_appointment.date_resigned are returned as NULL; otherwise the value is cast to DATE.",
                    "long_name": "Date Resigned",
                    "unit": "n/a"
                }
            },
            "is_active_appointment": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Derived flag indicating whether no resignation date is recorded. True when api_appointment.date_resigned is '-' or NULL; otherwise false.",
                    "long_name": "Is Active Appointment",
                    "unit": "boolean"
                }
            },
            "source_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The maximum data lake ingestion timestamp evaluated across the base appointment record and any successfully resolved appointee person or company dimension rows. This serves as the engineering sequence tracker for Delta Live Tables (DLT) Slowly Changing Dimensions (SCD2) history boundaries.",
                    "long_name": "Source Ingestion Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp when the record was processed into the Silver layer, generated using CURRENT_TIMESTAMP().",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            },

        }
    },

    "company_dim": {
        "table": {
            "comment": "Silver layer dimension table for legal entities. Provides core company/entity attributes sourced from GEMS, including identifiers, registration details, status, fund, portfolio, and technology mapping."
        },
        "columns": {
            "cdm_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM internal identifier for the entity.",
                    "long_name": "CDM Company ID",
                    "unit": "n/a"
                }
            },
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS company identifier used across the data model.",
                    "long_name": "GEMS Company ID",
                    "unit": "n/a"
                }
            },
            "gems_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the company or legal entity in GEMS.",
                    "long_name": "GEMS Company Name",
                    "unit": "n/a"
                }
            },
            "gems_company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The official company incorporation or registration number recorded in GEMS.",
                    "long_name": "GEMS Company Number",
                    "unit": "n/a"
                }
            },
            "gems_entity_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the external, legal corporate structure/form of the company (e.g., LLC, GmbH, S.A.).",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "gems_company_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the internal, commercial function or structural role the company plays within Octopus' corporate hierarchy (e.g., HoldCo, SPV, Fund).",
                    "long_name": "Company Type",
                    "unit": "n/a"
                }
            },
            "gems_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The current operational or legal status of the company recorded in GEMS.",
                    "long_name": "GEMS Status",
                    "unit": "n/a"
                }
            },
            "gems_registration_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the entity was officially registered, as recorded in GEMS.",
                    "long_name": "GEMS Registration Date",
                    "unit": "n/a"
                }
            },
            "gems_jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country or legal jurisdiction of registration recorded in GEMS.",
                    "long_name": "GEMS Jurisdiction",
                    "unit": "n/a"
                }
            },
            "gems_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund associated with the company or entity in GEMS.",
                    "long_name": "GEMS Fund",
                    "unit": "n/a"
                }
            },
            "gems_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investment portfolio associated with the company or entity in GEMS.",
                    "long_name": "GEMS Portfolio",
                    "unit": "n/a"
                }
            },
            "gems_technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The technology sector or asset class associated with the company or entity in GEMS.",
                    "long_name": "GEMS Technology",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp when this dimension record was ingested into the Data lake.",
                    "long_name": "Data Lake Ingestion Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp at which this model was refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company_address_dim": {
        "table": {
            "comment": "Silver layer dimension table containing company address information sourced from GEMS. Includes address identifiers, address hierarchy components, and geographic details for legal entities."
        },
        "columns": {
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS company identifier associated with the address record.",
                    "long_name": "GEMS Company ID",
                    "unit": "n/a"
                }
            },
            "gems_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered company name associated with the address record.",
                    "long_name": "GEMS Company Name",
                    "unit": "n/a"
                }
            },
            "gems_address_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique identifier for the address record in GEMS.",
                    "long_name": "GEMS Address ID",
                    "unit": "n/a"
                }
            },
            "gems_address_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The classification of the address, such as registered office or business address.",
                    "long_name": "GEMS Address Type",
                    "unit": "n/a"
                }
            },
            "gems_full_address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The complete formatted address string from GEMS.",
                    "long_name": "GEMS Full Address",
                    "unit": "n/a"
                }
            },
            "gems_suite_apt": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The suite, apartment, or unit information associated with the address.",
                    "long_name": "GEMS Suite/Apt",
                    "unit": "n/a"
                }
            },
            "gems_address_line_1": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The first line of the address.",
                    "long_name": "GEMS Address Line 1",
                    "unit": "n/a"
                }
            },
            "gems_address_line_2": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The second line of the address.",
                    "long_name": "GEMS Address Line 2",
                    "unit": "n/a"
                }
            },
            "gems_address_line_3": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The third line of the address.",
                    "long_name": "GEMS Address Line 3",
                    "unit": "n/a"
                }
            },
            "gems_city": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The city component of the address.",
                    "long_name": "GEMS City",
                    "unit": "n/a"
                }
            },
            "gems_county": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The county or district component of the address.",
                    "long_name": "GEMS County",
                    "unit": "n/a"
                }
            },
            "gems_state_province": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The state or province component of the address.",
                    "long_name": "GEMS State/Province",
                    "unit": "n/a"
                }
            },
            "gems_zip_post_code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The postal code or ZIP code associated with the address.",
                    "long_name": "GEMS ZIP/Post Code",
                    "unit": "n/a"
                }
            },
            "gems_country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country component of the address.",
                    "long_name": "GEMS Country",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp when this dimension record was ingested into the Data lake.",
                    "long_name": "Data Lake Ingestion Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp at which this model was refreshed.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "shareholdings_recon": {
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
            "gems_transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The type of transaction that occurred between the parent and child companies during this time slice.",
                    "long_name": "Transaction Type",
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

    "company_recon": {
        "table": {
            "comment": "Silver layer reconciliation table comparing CDM and GEMs company records. Performs a full outer join between the latest active CDM company mappings (aggregated to one row per company) and GEMs company dimension data to identify matched, missing, and mismatched records across IDs, names, company numbers, funds, and portfolios."
        },
        "columns": {
            "reconciled_cdm_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Resolved CDM company identifier, coalesced from CDM company_core_id, GEMs cdm_company_id, or the fallback value 'MISSING_IN_CDM'.",
                    "long_name": "Reconciled CDM ID",
                    "unit": "n/a"
                }
            },
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "GEMs company identifier, or the fallback value 'MISSING_IN_GEMS' where no GEMs record exists.",
                    "long_name": "GEMs Company ID",
                    "unit": "n/a"
                }
            },
            "id_match_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Categorical status showing whether the company matched on CDM ID, exists only in GEMs, or exists only in CDM.",
                    "long_name": "ID Match Status",
                    "unit": "n/a"
                }
            },
            "cdm_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Registered company name from bronze_company_dim_core.company_registered_name.",
                    "long_name": "CDM Company Name",
                    "unit": "n/a"
                }
            },
            "gems_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company name from the GEMs company dimension.",
                    "long_name": "GEMs Company Name",
                    "unit": "n/a"
                }
            },
            "is_name_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating whether the trimmed, lowercased CDM and GEMs company names match exactly.",
                    "long_name": "Is Name Exact Match",
                    "unit": "boolean"
                }
            },
            "name_similarity_score": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Normalized Levenshtein similarity score between CDM and GEMs company names. Returns 1.0 for identical or both-null names, 0.0 where only one name is null, otherwise calculated as 1 minus Levenshtein distance divided by the greatest trimmed name length.",
                    "long_name": "Name Similarity Score",
                    "unit": "decimal"
                }
            },
            "cdm_incorporation_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Trimmed CDM company registered incorporation number, with blank strings converted to NULL.",
                    "long_name": "CDM Incorporation Number",
                    "unit": "n/a"
                }
            },
            "gems_company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Trimmed GEMs company number, with blank strings converted to NULL.",
                    "long_name": "GEMs Company Number",
                    "unit": "n/a"
                }
            },
            "is_number_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating whether the trimmed CDM incorporation number and GEMs company number match exactly.",
                    "long_name": "Is Number Exact Match",
                    "unit": "boolean"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Comma-separated string of CDM fund display names (deduplicated and alphabetized) derived from the active valuation point mappings via investment portfolio and fund dimension joins.",
                    "long_name": "CDM Fund",
                    "unit": "n/a"
                }
            },
            "gems_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Trimmed GEMs fund value from the GEMs company dimension, with blank strings converted to NULL.",
                    "long_name": "GEMs Fund",
                    "unit": "n/a"
                }
            },
            "is_fund_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating whether the trimmed, lowercased CDM fund string and GEMs fund values match exactly.",
                    "long_name": "Is Fund Exact Match",
                    "unit": "boolean"
                }
            },
            "cdm_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Comma-separated string of CDM investment portfolio names (deduplicated and alphabetized) derived from the active valuation point mappings.",
                    "long_name": "CDM Portfolio",
                    "unit": "n/a"
                }
            },
            "gems_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Trimmed GEMs portfolio value from the GEMs company dimension, with blank strings converted to NULL.",
                    "long_name": "GEMs Portfolio",
                    "unit": "n/a"
                }
            },
            "is_portfolio_exact_match": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating whether the trimmed, lowercased CDM portfolio string and GEMs portfolio values match exactly.",
                    "long_name": "Is Portfolio Exact Match",
                    "unit": "boolean"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp when the reconciliation record was computed, generated using current_timestamp().",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "shareholdings_fact": {
        "table": {
            "comment": "Silver layer fact table recording historical, contiguous time-sliced ownership percentages. It computes ownership by aggregating transaction values weighted by financial security types (SCD2) and unions this with membership guarantee data to provide a unified view of company control."
        },
        "columns": {
            "company_bridge_parent_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A unique, deterministic SHA-256 hash generated from the child ID, parent ID, and active from date. Used as a primary key for incremental loading and tracking specific state slices.",
                    "long_name": "Company Bridge Parent Hash ID",
                    "unit": "n/a"
                }
            },
            "child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The system-specific identifier (GEMs Company ID) for the child company being owned.",
                    "long_name": "Child Company System ID",
                    "unit": "n/a"
                }
            },
            "internal_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the child company.",
                    "long_name": "Child Company Internal ID",
                    "unit": "n/a"
                }
            },
            "parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The system-specific identifier (GEMs Company ID) for the parent/holding entity.",
                    "long_name": "Parent Company System ID",
                    "unit": "n/a"
                }
            },
            "internal_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The universal internal identifier (Core Data Model ID) for the parent/holding entity.",
                    "long_name": "Parent Company Internal ID",
                    "unit": "n/a"
                }
            },
            "ownership_percentage": {
                "schema": {"data_type": "FLOAT"},
                "description": {
                    "comment": "The calculated percentage of ownership. For shareholdings, this is (weighted units / total outstanding units) * financial weight. For guarantees, this is the stated percentage held.",
                    "long_name": "Ownership Percentage",
                    "unit": "%"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The type of event that triggered this ownership slice. Includes specific share transaction types, 'Passive Dilution/Accretion' for mathematical shifts, or 'Partnership Appointment' for guarantees.",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The start date of this specific ownership percentage slice, derived from either the transaction date or the appointment date.",
                    "long_name": "Active From Date",
                    "unit": "n/a"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The end date of this specific ownership slice. For shareholdings, this is the date of the next transaction (lead); for guarantees, it is the resignation date. Null indicates a current relationship.",
                    "long_name": "Active To Date",
                    "unit": "n/a"
                }
            },
            "source_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The cumulative maximum data lake ingestion timestamp carried forward through the transaction timeline or matching membership guarantee data. Represents the latest system modification event contributing to the aggregated percentage slice and dictates DLT SCD2 sequencing logic.",
                    "long_name": "Source Ingestion Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver fact record was computed by the shareholdings engine.",
                    "long_name": "Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company_relationships_fact": {
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

    "mandate_to_assetco_relationships_recon": {
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

    "company_relationships_recon": {
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