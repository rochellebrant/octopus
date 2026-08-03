gold_layer_definitions = {
    "mandate_to_company_fact": {
        "table": {
            "comment": "Gold layer business-facing view providing the current active ownership relationships between fund mandate entities and the companies they directly or indirectly own. The view filters the broader company ownership relationship dataset to include only relationships where the ultimate parent entity is recognised as an active fund within the Core Data Model (CDM), and where the ownership relationship is active as of the current date. Each record represents a live ownership link between a mandate or fund structure and an owned company, including ownership percentage, transaction type, relationship dates, and associated investment portfolio and fund metadata. The view is intended to support operational reporting, mandate oversight, portfolio ownership analysis, legal entity monitoring, and downstream analytics requiring a current-state representation of fund-to-company ownership structures."
        },
        "columns": {
            "mandate_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The internal company identifier for the mandate or fund entity acting as the ultimate parent in the ownership structure.",
                    "long_name": "Mandate Company ID",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The internal company identifier for the owned company within the active ownership structure.",
                    "long_name": "Owned Company ID",
                    "unit": "n/a"
                }
            },
            "mandate_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered company name of the mandate or fund entity acting as the ultimate parent in the ownership relationship.",
                    "long_name": "Mandate Name",
                    "unit": "n/a"
                }
            },
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered company name of the owned company within the active ownership structure.",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The originating source system for the ownership relationship record, such as GEMs or CDM.",
                    "long_name": "Data Source",
                    "unit": "n/a"
                }
            },
            "percentage_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The effective ownership percentage held by the mandate or fund entity in the owned company.",
                    "long_name": "Percentage Ownership",
                    "unit": "percent"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The ownership transaction classification associated with the relationship, such as acquisition or disposal.",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the ownership relationship became active or effective.",
                    "long_name": "Relationship Start Date",
                    "unit": "date"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the ownership relationship ended. NULL indicates the relationship is currently active.",
                    "long_name": "Relationship End Date",
                    "unit": "date"
                }
            },
            "cdm_investment_portfolio_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of Core Data Model investment portfolio identifiers associated with the ownership relationship.",
                    "long_name": "CDM Investment Portfolio IDs",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of Core Data Model investment portfolio names associated with the ownership relationship.",
                    "long_name": "CDM Investment Portfolio Names",
                    "unit": "n/a"
                }
            },
            "cdm_fund_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of Core Data Model fund identifiers associated with the ownership relationship.",
                    "long_name": "CDM Fund IDs",
                    "unit": "n/a"
                }
            },
            "cdm_fund_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of Core Data Model fund names associated with the ownership relationship.",
                    "long_name": "CDM Fund Names",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp when the record was last refreshed in the Gold layer.",
                    "long_name": "Refresh Timestamp",
                    "unit": "timestamp"
                }
            }
        },
    },
    "company_appointments_fact": {
        "table": {
            "comment": "Gold layer business-facing view providing a consolidated record of company appointments, directorships, and officer relationships across entities managed within the GEMs and Core Data Model (CDM) platforms. The view combines company identity information with appointment details and appointee demographic attributes to support governance reporting, entity management, compliance oversight, operational monitoring, and diversity analysis. Each record represents an appointment relationship between a company and an individual or entity, including the appointment role, appointment lifecycle dates, and indicators showing whether the appointment is currently active. The dataset enriches appointment records with company metadata such as company number, company type, and jurisdiction, alongside person-level attributes including name, gender, and nationality. The view is designed to provide a current operational view of active and historical appointments while supporting downstream legal entity reporting, board composition analysis, and regulatory governance processes."
        },
        "columns": {
            # --- 1. Identifiers ---
            "cdm_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unified internal identifier for the company.",
                    "long_name": "Company Internal ID",
                    "unit": "n/a"
                }
            },
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The source system identifier (GEMS) for the company.",
                    "long_name": "GEMS Company ID",
                    "unit": "n/a"
                }
            },
            "cdm_appointee_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unified internal identifier for the appointed person.",
                    "long_name": "Appointee Internal ID",
                    "unit": "n/a"
                }
            },
            "gems_appointee_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The source system identifier (GEMS) for the appointed person.",
                    "long_name": "GEMS Appointee ID",
                    "unit": "n/a"
                }
            },

            # --- 2. Company Descriptive Data ---
            "gems_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered company name as recorded in GEMs.",
                    "long_name": "GEMS Company Name",
                    "unit": "n/a"
                }
            },
            "gems_company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The official company registration or incorporation number recorded in GEMs.",
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
            "gems_jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legal jurisdiction under which the company is registered.",
                    "long_name": "GEMS Jurisdiction",
                    "unit": "n/a"
                }
            },

            # --- 3. Appointee Descriptive Data ---
            "gems_appointment_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific role or title of the appointee (e.g., 'Director', 'Company Secretary').",
                    "long_name": "GEMS Appointment Type",
                    "unit": "n/a"
                }
            },
            "gems_appointee_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full name of the appointed individual. DYNAMICALLY MASKED: Evaluates to '*** MASKED ***' for users outside the 'Sennen XIO Project' group.",
                    "long_name": "GEMS Appointee Name",
                    "unit": "n/a"
                }
            },
            "gender": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The recorded gender of the appointee. Unmasked for aggregate diversity reporting.",
                    "long_name": "Gender",
                    "unit": "n/a"
                }
            },
            "nationality": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The recorded nationality of the appointee. Unmasked for aggregate demographic reporting.",
                    "long_name": "Nationality",
                    "unit": "n/a"
                }
            },

            # --- 4. Boolean Flags ---
            "is_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Flag indicating if the appointment record itself is considered active.",
                    "long_name": "Is Active Record",
                    "unit": "boolean"
                }
            },
            "is_active_appointment": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Flag indicating if the appointment is currently active (True) or if the current date falls outside the appointment window (False).",
                    "long_name": "Is Active Appointment",
                    "unit": "boolean"
                }
            },

            # --- 5. Business Dates ---
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The official date the individual was appointed to the role.",
                    "long_name": "Date Appointed",
                    "unit": "date"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the appointment record ceased to be active. Hyphens in source indicate an open-ended date.",
                    "long_name": "Active To Date",
                    "unit": "date"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The official date the individual resigned or was removed from the role.",
                    "long_name": "Date Resigned",
                    "unit": "date"
                }
            },

            # --- 6. Metadata ---
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
                    "comment": "The timestamp of when this record was last refreshed in the Gold layer.",
                    "long_name": "Refresh Timestamp",
                    "unit": "timestamp"
                }
            },
        },
    },

    "company_relationships_fact": {
        "table": {
            "comment": "Gold layer business-facing fact table providing a calculated view of company ownership relationships across direct and indirect shareholding chains. The table uses source shareholding records to trace ownership paths between parent and child companies, including multi-level relationships where ownership flows through one or more intermediate entities. Each record represents a versioned ownership relationship at a point in time, showing the ultimate parent company, ultimate child company, effective ownership percentage, transaction date, relationship depth, and transaction type. This supports group structure reporting, ownership analysis, legal entity oversight, governance monitoring, and downstream reporting that requires visibility of both simple and complex ownership chains. The table preserves historical relationship states so changes in ownership can be tracked over time rather than only showing the current structure."
        },
        "columns": {
            # --- 1. Identifiers ---
            "rel_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique identifier for the parent-child company relationship.",
                    "long_name": "Relationship ID",
                    "unit": "n/a"
                }
            },
            "ultimate_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unified internal identifier for the ultimate parent company in the ownership chain.",
                    "long_name": "Ultimate Parent Company ID",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unified internal identifier for the ultimate child (subsidiary) company in the ownership chain.",
                    "long_name": "Ultimate Child Company ID",
                    "unit": "n/a"
                }
            },

            # --- 2. Descriptive Data ---
            "ultimate_parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate parent company.",
                    "long_name": "Ultimate Parent Company Name",
                    "unit": "n/a"
                }
            },
            "ultimate_child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the ultimate child (subsidiary) company.",
                    "long_name": "Ultimate Child Company Name",
                    "unit": "n/a"
                }
            },
            "company_path_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Ordered array of company names representing the full ownership path from parent to child.",
                    "long_name": "Company Ownership Path",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The originating data source for this relationship record (e.g., 'GEMs', 'CDM').",
                    "long_name": "Data Source",
                    "unit": "n/a"
                }
            },
            "transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The type of ownership transaction recorded (e.g., acquisition, disposal).",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },

            # --- 3. Numeric Measures ---
            "percentage_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of ownership the parent holds in the child company.",
                    "long_name": "Percentage Ownership",
                    "unit": "percent"
                }
            },

            # --- 4. Business Dates ---
            "active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the ownership relationship became effective.",
                    "long_name": "Relationship Start Date",
                    "unit": "date"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the ownership relationship ended. For CDM-sourced records, this may be truncated to the GEMs waterline date to prevent overlap.",
                    "long_name": "Relationship End Date",
                    "unit": "date"
                }
            },

            # --- 5. CDM Cross-References ---
            "cdm_investment_portfolio_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of CDM investment portfolio identifiers associated with this relationship.",
                    "long_name": "CDM Investment Portfolio IDs",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of CDM investment portfolio names associated with this relationship.",
                    "long_name": "CDM Investment Portfolio Names",
                    "unit": "n/a"
                }
            },
            "cdm_fund_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of CDM fund identifiers associated with this relationship.",
                    "long_name": "CDM Fund IDs",
                    "unit": "n/a"
                }
            },
            "cdm_fund_names": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "Array of CDM fund names associated with this relationship.",
                    "long_name": "CDM Fund Names",
                    "unit": "n/a"
                }
            },

            # --- 6. Metadata ---
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp of when this record was last refreshed in the Gold layer.",
                    "long_name": "Refresh Timestamp",
                    "unit": "timestamp"
                }
            },
        },
    },

    "company_dim": {
        "table": {
            "comment": "Gold layer business-facing view providing a consolidated register of company information across both the GEMs and Core Data Model (CDM) platforms. The view combines company identity, registration, status, jurisdiction, and address information into a single standardised structure to support reporting, operational oversight, reconciliation, and downstream analytics. Records sourced from GEMs include enriched company metadata and registered address details, while additional CDM-only companies are included to provide broader coverage of entities that may not yet exist in GEMs. The view is designed to present the latest active representation of each company and to provide a unified reference point for legal entity and portfolio-related reporting."
        },
        "columns": {
            # --- 1. Identifiers ---
            "cdm_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unified internal identifier for the company. Sourced from GEMS internal_id or CDM company_core_id.",
                    "long_name": "CDM Company ID",
                    "unit": "n/a"
                }
            },
            "gems_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS source system identifier for the company. NULL for CDM-only records.",
                    "long_name": "GEMS Company ID",
                    "unit": "n/a"
                }
            },

            # --- 2. Descriptive Data ---
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The registered name of the company.",
                    "long_name": "Company Registered Name",
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
                    "comment": "The current operational status of the company (e.g., 'Active', 'Dissolved'). NULL for CDM-only records.",
                    "long_name": "Company Status",
                    "unit": "n/a"
                }
            },
            "company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The official company registration or incorporation number.",
                    "long_name": "Company Registration Number",
                    "unit": "n/a"
                }
            },
            "jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legal jurisdiction under which the company is registered. NULL for CDM-only records.",
                    "long_name": "Jurisdiction",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The originating data source for this record ('GEMs' or 'CDM').",
                    "long_name": "Data Source",
                    "unit": "n/a"
                }
            },

            # --- 3. Address Data ---
            "address_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The type of address recorded (e.g., 'Registered Office'). NULL for CDM-only records.",
                    "long_name": "Address Type",
                    "unit": "n/a"
                }
            },
            "full_address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The complete address as a single concatenated string. NULL for CDM-only records.",
                    "long_name": "Full Address",
                    "unit": "n/a"
                }
            },
            "city": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The city component of the company address. NULL for CDM-only records.",
                    "long_name": "City",
                    "unit": "n/a"
                }
            },
            "county": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The county component of the company address. NULL for CDM-only records.",
                    "long_name": "County",
                    "unit": "n/a"
                }
            },
            "state_province": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The state or province component of the company address. NULL for CDM-only records.",
                    "long_name": "State / Province",
                    "unit": "n/a"
                }
            },
            "zip_post_code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The ZIP or postal code component of the company address. NULL for CDM-only records.",
                    "long_name": "ZIP / Post Code",
                    "unit": "n/a"
                }
            },
            "country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country of the company address. For CDM-only records, derived from the country dimension lookup.",
                    "long_name": "Country",
                    "unit": "n/a"
                }
            },

            # --- 4. Business Dates ---
            "registration_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the company was officially registered or incorporated.",
                    "long_name": "Registration Date",
                    "unit": "date"
                }
            },

            # --- 5. Metadata ---
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp of when this record was last refreshed in the Gold layer. NULL for CDM-only records.",
                    "long_name": "Refresh Timestamp",
                    "unit": "timestamp"
                }
            }
        },
    },

    "shareholdings_fact": {
        "table": {
            "comment": "Gold layer business-facing view providing a reconciled timeline of direct company shareholding relationships. The view combines GEMs and Core Data Model (CDM) ownership records into a single standardised structure, using GEMs as the preferred source where available and retaining CDM records where they provide earlier historical context before GEMs coverage begins. Each record represents a direct parent-to-child shareholding relationship, including the child company, parent company, ownership percentage, transaction type, effective start date, and end date. The view is designed to avoid overlapping ownership timelines between GEMs and CDM by limiting CDM records once GEMs data becomes available for the same relationship. Current open-ended relationships are represented with a NULL end date. This supports ownership reporting, entity structure analysis, reconciliation, governance oversight, and downstream calculations that rely on a clean direct shareholding history."
        },
        "columns": {
            # --- 1. Identifiers ---
            "cdm_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM internal identifier for the child (subsidiary) company.",
                    "long_name": "CDM Child Company ID",
                    "unit": "n/a"
                }
            },
            "cdm_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM internal identifier for the parent (holding) company.",
                    "long_name": "CDM Parent Company ID",
                    "unit": "n/a"
                }
            },
            "gems_child_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS source system identifier for the child (subsidiary) company.",
                    "long_name": "GEMS Child Company ID",
                    "unit": "n/a"
                }
            },
            "gems_parent_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS source system identifier for the parent (holding) company.",
                    "long_name": "GEMS Parent Company ID",
                    "unit": "n/a"
                }
            },

            # --- 2. Descriptive Data ---
            "child_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the child company. Resolved as COALESCE(gems_name, cdm_name).",
                    "long_name": "Child Company Name",
                    "unit": "n/a"
                }
            },
            "parent_company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the parent company. Resolved as COALESCE(gems_name, cdm_name).",
                    "long_name": "Parent Company Name",
                    "unit": "n/a"
                }
            },
            "gems_transaction_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The GEMS transaction type for the shareholding event (e.g., acquisition, disposal).",
                    "long_name": "GEMS Transaction Type",
                    "unit": "n/a"
                }
            },
            "source": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The originating data source for this record ('GEMs' if gems_ownership_percentage is present, otherwise 'CDM').",
                    "long_name": "Data Source",
                    "unit": "n/a"
                }
            },

            # --- 3. Numeric Measures ---
            "ownership_percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of ownership the parent holds in the child company. Resolved as COALESCE(gems_ownership_percentage, cdm_ownership_percentage).",
                    "long_name": "Ownership Percentage",
                    "unit": "percent"
                }
            },

            # --- 4. Business Dates ---
            "active_from_date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The date the shareholding became effective (comparison_slice_start).",
                    "long_name": "Shareholding Start Date",
                    "unit": "date"
                }
            },
            "active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date the shareholding ended. NULL if this is the current open-ended record (max boundary restored to NULL).",
                    "long_name": "Shareholding End Date",
                    "unit": "date"
                }
            },

            # --- 5. CDM Cross-References ---
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM fund identifier associated with this shareholding.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM fund name associated with this shareholding.",
                    "long_name": "CDM Fund Name",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM investment portfolio identifier associated with this shareholding.",
                    "long_name": "CDM Investment Portfolio ID",
                    "unit": "n/a"
                }
            },
            "cdm_investment_portfolio_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The CDM investment portfolio name associated with this shareholding.",
                    "long_name": "CDM Investment Portfolio Name",
                    "unit": "n/a"
                }
            },

            # --- 6. Metadata ---
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The timestamp of when this record was last refreshed in the Gold layer.",
                    "long_name": "Refresh Timestamp",
                    "unit": "timestamp"
                }
            },
        },
    },
}
