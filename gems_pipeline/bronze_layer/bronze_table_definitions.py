api_field_mapping = { 
    "api_appointment": {
        "entitycounter": "primaryentitykeyid", 
        "company_id": "1086760",
        "company_internal_id": "100000578",
        "jurisdiction": "856804",
        "company_name": "1079955",
        "appointment_type": "882515",
        "appointee_entity_id": "1088587",
        "appointee_internal_id": "100000575",
        "appointed_entity_name": "882517",
        "date_appointed": "882495",
        "date_resigned": "882510",
        "is_independent_director": "1019916"
    },
    "api_person": {
        "entitycounter": "primaryentitykeyid",
        "internal_id": "100000575",
        "entity_id": "1086800",
        "entity_type": "375424",
        "name": "375419",
        "gender": "1070657",
        "address_type": "1070642",
        "full_address": "1070643",
        "nationality": "1070656",
        "birth_date": "1070655",
        "ch_director_code": "100000513"
    },
    "api_people": {
        "entitycounter": "primaryentitykeyid",
        "person_id": "1086765",
        "internal_id": "100000649",
        "full_name": "1080208",
        "surname": "859773",
        "first_name": "859771",
        "second_name": "859766",
        "third_name": "859764",
        "fourth_name": "859762",
        "email": "859739"
    },
    "api_company_address": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "1086760",
        "company_name": "856806",
        "address_id": "1089710",
        "address_type": "575808",
        "full_address": "1086027",
        "suite_apt": "650080",
        "address_line_1": "650078",
        "address_line_2": "650076",
        "address_line_3": "650074",
        "city": "650072",
        "county": "650070",
        "state_province": "650069",
        "zip_post_code": "650067",
        "country": "650064"
    },
    "api_entity": {
        "entitycounter": "primaryentitykeyid",
        "internal_id": "100000578",
        "company_id": "1086760",
        "name": "856806",
        "company_type": "100000562",
        "entity_type": "856792",
        "status": "856768",
        "company_number": "856795",
        "registration_date": "856801",
        "jurisdiction": "856804",
        "fund": "100000594",
        "portfolio": "100000456",
        "technology": "100000609",
        "archived": "856784"
    },
    "api_member_guarantees": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "1086760",
        "company_internal_id": "100000578",
        "company_name": "856806",
        "member_name": "873301",
        "membership_type": "873298",
        "units_held": "873289",
        "percentage_held": "873296",
        "date_appointed": "873292",
        "date_resigned": "873290"
    },
    "api_shareholdings": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "1086760",
        "company_name": "856806",
        "transaction_id": "1082518",
        "registered_holder": "1077283",
        "beneficial_holder": "1077284",
        "security": "1077310",
        "type_of_transaction": "1077389",
        "date_of_transaction": "1077387",
        "nominal_value": "1077318",
        "amount_of_transaction": "1077388",
        "value_of_transaction": "1077390",
        "financial_weight_per_security": "1077349",
        "registered_holder_percent": "1077368",
        "transaction_notes": "1077392"
    },
}

column_definitions = {
    "api_appointment": {
        "table": {
            "comment": "Data table for company appointments and roles."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "company_internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID of the company",
                    "long_name": "Company Internal ID",
                    "unit": "n/a"
                }
            },
            "jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Jurisdiction where the company is registered",
                    "long_name": "Jurisdiction",
                    "unit": "n/a"
                }
            },
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "appointment_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Role or type of appointment",
                    "long_name": "Appointment Type",
                    "unit": "n/a"
                }
            },
            "appointee_entity_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Entity ID of the appointed person or company",
                    "long_name": "Appointee Entity ID",
                    "unit": "n/a"
                }
            },
            "appointee_internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID of the appointed person or company",
                    "long_name": "Appointee Internal ID",
                    "unit": "n/a"
                }
            },
            "appointed_entity_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the appointed person or company",
                    "long_name": "Appointed Entity Name",
                    "unit": "n/a"
                }
            },
            "date_appointed": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the appointment started",
                    "long_name": "Date Appointed",
                    "unit": "n/a"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the appointment ended or resigned",
                    "long_name": "Date Resigned",
                    "unit": "n/a"
                }
            },
            "is_independent_director": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Indicates if the appointee is an independent director",
                    "long_name": "Is Independent Director",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_person": {
        "table": {
            "comment": "Data table containing person entity details."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID from GEMS",
                    "long_name": "Internal ID",
                    "unit": "n/a"
                }
            },
            "entity_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Entity identifier",
                    "long_name": "Entity ID",
                    "unit": "n/a"
                }
            },
            "entity_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the external, legal corporate structure/form of the company (e.g., LLC, GmbH, S.A.).",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Full name of the person",
                    "long_name": "Name",
                    "unit": "n/a"
                }
            },
            "gender": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Gender of the person",
                    "long_name": "Gender",
                    "unit": "n/a"
                }
            },
            "address_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of address provided",
                    "long_name": "Address Type",
                    "unit": "n/a"
                }
            },
            "full_address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Full residential or service address",
                    "long_name": "Full Address",
                    "unit": "n/a"
                }
            },
            "nationality": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Nationality of the person",
                    "long_name": "Nationality",
                    "unit": "n/a"
                }
            },
            "birth_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date of birth of the person",
                    "long_name": "Birth Date",
                    "unit": "n/a"
                }
            },
            "ch_director_code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Companies House Director Code",
                    "long_name": "CH Director Code",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_people": {
        "table": {
            "comment": "Data table containing detailed name components for people."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "person_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Unique identifier for the person",
                    "long_name": "Person ID",
                    "unit": "n/a"
                }
            },
            "internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID from GEMS",
                    "long_name": "Internal ID",
                    "unit": "n/a"
                }
            },
            "full_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Full name of the person",
                    "long_name": "Full Name",
                    "unit": "n/a"
                }
            },
            "surname": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Surname or family name",
                    "long_name": "Surname",
                    "unit": "n/a"
                }
            },
            "first_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "First given name",
                    "long_name": "First Name",
                    "unit": "n/a"
                }
            },
            "second_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Second given name or middle name",
                    "long_name": "Second Name",
                    "unit": "n/a"
                }
            },
            "third_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Third given name",
                    "long_name": "Third Name",
                    "unit": "n/a"
                }
            },
            "fourth_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Fourth given name",
                    "long_name": "Fourth Name",
                    "unit": "n/a"
                }
            },
            "email": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Email address associated with the person",
                    "long_name": "Email",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_company_address": {
        "table": {
            "comment": "Data table containing company address details."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "address_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Unique identifier for the specific address",
                    "long_name": "Address ID",
                    "unit": "n/a"
                }
            },
            "address_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of address (e.g., Registered Office)",
                    "long_name": "Address Type",
                    "unit": "n/a"
                }
            },
            "full_address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Complete formatted address",
                    "long_name": "Full Address",
                    "unit": "n/a"
                }
            },
            "suite_apt": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Suite or Apartment number",
                    "long_name": "Suite / Apt #",
                    "unit": "n/a"
                }
            },
            "address_line_1": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "First line of the address",
                    "long_name": "Address Line 1",
                    "unit": "n/a"
                }
            },
            "address_line_2": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Second line of the address",
                    "long_name": "Address Line 2",
                    "unit": "n/a"
                }
            },
            "address_line_3": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Third line of the address",
                    "long_name": "Address Line 3",
                    "unit": "n/a"
                }
            },
            "city": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "City of the address",
                    "long_name": "City",
                    "unit": "n/a"
                }
            },
            "county": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "County of the address",
                    "long_name": "County",
                    "unit": "n/a"
                }
            },
            "state_province": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "State or Province of the address",
                    "long_name": "State/Province",
                    "unit": "n/a"
                }
            },
            "zip_post_code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Postal or Zip code",
                    "long_name": "Zip/Post Code",
                    "unit": "n/a"
                }
            },
            "country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country of the address",
                    "long_name": "Country",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_entity": {
        "table": {
            "comment": "Data table for general entity properties and metadata."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID from GEMS",
                    "long_name": "Internal ID",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company/entity",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "company_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the internal, commercial function or structural role the company plays within Octopus' corporate hierarchy (e.g., HoldCo, SPV, Fund).",
                    "long_name": "Company Type",
                    "unit": "n/a"
                }
            },
            "entity_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Represents the external, legal corporate structure/form of the company (e.g., LLC, GmbH, S.A.).",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Current operational status of the entity",
                    "long_name": "Status",
                    "unit": "n/a"
                }
            },
            "company_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Official registration number",
                    "long_name": "Company Number",
                    "unit": "n/a"
                }
            },
            "registration_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the entity was registered",
                    "long_name": "Registration Date",
                    "unit": "n/a"
                }
            },
            "jurisdiction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Jurisdiction where the entity is registered",
                    "long_name": "Jurisdiction",
                    "unit": "n/a"
                }
            },
            "fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Fund associated with the entity",
                    "long_name": "Fund",
                    "unit": "n/a"
                }
            },
            "portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Portfolio the entity belongs to",
                    "long_name": "Portfolio",
                    "unit": "n/a"
                }
            },
            "technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Technology or sector associated with the entity",
                    "long_name": "Technology",
                    "unit": "n/a"
                }
            },
            "archived": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates if the entity is archived",
                    "long_name": "Archived",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_member_guarantees": {
        "table": {
            "comment": "Data table for member guarantees and unit holdings."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "company_internal_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal ID from GEMS",
                    "long_name": "Internal ID",
                    "unit": "n/a"
                }
            },
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company/entity",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "member_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the member or guarantor",
                    "long_name": "Member Name",
                    "unit": "n/a"
                }
            },
            "membership_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of membership or partnership",
                    "long_name": "Membership Type",
                    "unit": "n/a"
                }
            },
            "units_held": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Number of units held by the member",
                    "long_name": "Units Held",
                    "unit": "count"
                }
            },
            "percentage_held": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Percentage of the company held by the member",
                    "long_name": "Percentage Held",
                    "unit": "%"
                }
            },
            "date_appointed": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the member was appointed",
                    "long_name": "Date Appointed",
                    "unit": "n/a"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the member resigned (if applicable)",
                    "long_name": "Date Resigned",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    },

    "api_shareholdings": {
        "table": {
            "comment": "Data table tracking shareholding transactions and ownership."
        },
        "columns": {
            "entitycounter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary entity key",
                    "long_name": "Entity Counter",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "company_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "transaction_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Unique identifier for the transaction",
                    "long_name": "Transaction ID",
                    "unit": "n/a"
                }
            },
            "registered_holder": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Registered holder of the shares",
                    "long_name": "Registered Holder",
                    "unit": "n/a"
                }
            },
            "beneficial_holder": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Beneficial holder of the shares",
                    "long_name": "Beneficial Holder",
                    "unit": "n/a"
                }
            },
            "security": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Security type or class",
                    "long_name": "Security",
                    "unit": "n/a"
                }
            },
            "type_of_transaction": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of shareholding transaction",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "date_of_transaction": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date the transaction occurred",
                    "long_name": "Transaction Date",
                    "unit": "n/a"
                }
            },
            "nominal_value": {
                "schema": {"data_type": "DECIMAL(38, 4)"},
                "description": {
                    "comment": "Nominal value of the shares",
                    "long_name": "Nominal Value",
                    "unit": "Currency"
                }
            },
            "amount_of_transaction": {
                "schema": {"data_type": "DECIMAL(38, 4)"},
                "description": {
                    "comment": "Amount of shares in the transaction",
                    "long_name": "Transaction Amount",
                    "unit": "count"
                }
            },
            "value_of_transaction": {
                "schema": {"data_type": "DECIMAL(38, 4)"},
                "description": {
                    "comment": "Financial value of the transaction",
                    "long_name": "Transaction Value",
                    "unit": "Currency"
                }
            },
            "financial_weight_per_security": {
                "schema": {"data_type": "DECIMAL(38, 4)"},
                "description": {
                    "comment": "Financial weighting value of the security",
                    "long_name": "Financial Weight Per Security",
                    "unit": "Currency"
                }
            },
            "registered_holder_percent": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Registered Holder % Parent Interest Held",
                    "long_name": "Registered Holder Percent",
                    "unit": "%"
                }
            },
            "transaction_notes": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Notes regarding the transaction",
                    "long_name": "Transaction Notes",
                    "unit": "n/a"
                }
            },
            "table_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Source table saved search ID",
                    "long_name": "Table ID",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the landing layer",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_hash": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "SHA-256 hash of the row for SCD2 change tracking",
                    "long_name": "Row Hash",
                    "unit": "n/a"
                }
            }
        }
    }
}