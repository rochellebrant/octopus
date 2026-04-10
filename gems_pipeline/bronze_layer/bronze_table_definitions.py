api_field_mapping = {
    "api_appointment": {
        "entitycounter": "primaryentitykeyid", 
        "company_id": "companyBasics_3771_1_1086760",
        "company_internal_id": "companyBasics_3771_2_100000578",
        "jurisdiction": "companyBasics_3771_3_856804",
        "company_name": "companyAppointments_3488_4_1079955",
        "appointment_type": "companyAppointments_3488_5_882515",
        "appointee_entity_id": "companyAppointments_3488_6_1088587",
        "appointee_internal_id": "addEntity_3488_3521_7_100000575",
        "appointed_entity_name": "companyAppointments_3488_8_882517",
        "date_appointed": "companyAppointments_3488_9_882495",
        "date_resigned": "companyAppointments_3488_10_882510",
        "is_independent_director": "companyAppointments_3488_11_1019916"
    },
    "api_person": {
        "entitycounter": "primaryentitykeyid",
        "internal_id": "addEntity_0_1_100000575",
        "entity_id": "addEntity_0_2_1086800",
        "entity_type": "addEntity_0_3_375424",
        "name": "addEntity_0_4_375419",
        "gender": "addEntity_0_5_1070657",
        "address_type": "addEntity_0_6_1070642",
        "full_address": "addEntity_0_7_1070643",
        "nationality": "addEntity_0_8_1070656",
        "birth_date": "addEntity_0_9_1070655",
        "ch_director_code": "addEntity_0_10_100000513"
    },
    "api_people": {
        "entitycounter": "primaryentitykeyid",
        "person_id": "personBasics_0_1_1086765",
        "internal_id": "personBasics_0_2_100000649",
        "full_name": "personBasics_0_3_1080208",
        "surname": "personBasics_0_4_859773",
        "first_name": "personBasics_0_5_859771",
        "second_name": "personBasics_0_6_859766",
        "third_name": "personBasics_0_7_859764",
        "fourth_name": "personBasics_0_8_859762",
        "email": "personBasics_0_9_859739"
    },
    "api_company_address": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "companyBasics_0_1_1086760",
        "company_name": "companyBasics_0_2_856806",
        "address_id": "entityAddress_6_3_1089710",
        "address_type": "entityAddress_6_4_575808",
        "full_address": "address_6_1524_5_1086027",
        "suite_apt": "address_6_1524_6_650080",
        "address_line_1": "address_6_1524_7_650078",
        "address_line_2": "address_6_1524_8_650076",
        "address_line_3": "address_6_1524_9_650074",
        "city": "address_6_1524_10_650072",
        "county": "address_6_1524_11_650070",
        "state_province": "address_6_1524_12_650069",
        "zip_post_code": "address_6_1524_13_650067",
        "country": "address_6_1524_14_650064"
    },
    "api_entity": {
        "entitycounter": "primaryentitykeyid",
        "internal_id": "companyBasics_0_1_100000578",
        "company_id": "companyBasics_0_2_1086760",
        "name": "companyBasics_0_3_856806",
        "entity_type": "companyBasics_0_4_856792",
        "status": "companyBasics_0_5_856768",
        "company_number": "companyBasics_0_6_856795",
        "registration_date": "companyBasics_0_7_856801",
        "jurisdiction": "companyBasics_0_8_856804",
        "fund": "companyBasics_0_9_100000594",
        "portfolio": "companyBasics_0_10_100000456",
        "technology": "companyBasics_0_11_100000609",
        "archived": "companyBasics_0_12_856784"
    },
    "api_member_guarantees": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "companyBasics_0_1_1086760",
        "company_internal_id": "companyBasics_0_2_100000578",
        "company_name": "companyBasics_0_3_856806",
        "member_name": "memberGuarantee_55_5_873301",
        "membership_type": "memberGuarantee_55_6_873298",
        "units_held": "memberGuarantee_55_7_873289",
        "percentage_held": "memberGuarantee_55_8_873296",
        "date_appointed": "memberGuarantee_55_9_873292",
        "date_resigned": "memberGuarantee_55_10_873290"
    },
    "api_shareholdings": {
        "entitycounter": "primaryentitykeyid",
        "company_id": "companyBasics_0_1_1086760",
        "company_name": "companyBasics_0_2_856806",
        "transaction_id": "summaryShareholders_3367_4_1082518",
        "registered_holder": "summaryShareholders_3367_5_1077283",
        "beneficial_holder": "summaryShareholders_3367_6_1077284", #  use as parent company, not registered holder!
        "security": "summaryShareholders_3367_7_1077310",
        "type_of_transaction": "summaryShareholders_3367_8_1077389",
        "date_of_transaction": "summaryShareholders_3367_9_1077387",
        "nominal_value": "summaryShareholders_3367_10_1077318",
        "amount_of_transaction": "summaryShareholders_3367_11_1077388",
        "value_of_transaction": "summaryShareholders_3367_12_1077390",
        "financial_weight_per_security": "summaryShareholders_3367_14_1077349",
        "registered_holder_percent": "summaryShareholders_3367_13_1077368",
        "transaction_notes": "summaryShareholders_3367_15_1077392"
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
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date the appointment started",
                    "long_name": "Date Appointed",
                    "unit": "n/a"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date the appointment ended or resigned",
                    "long_name": "Date Resigned",
                    "unit": "n/a"
                }
            },
            "is_independent_director": {
                "schema": {"data_type": "STRING"},
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
                    "comment": "Type of entity (e.g., Person)",
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
                "schema": {"data_type": "TIMESTAMP"},
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
            "entity_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of entity",
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
                "schema": {"data_type": "TIMESTAMP"},
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
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date the member was appointed",
                    "long_name": "Date Appointed",
                    "unit": "n/a"
                }
            },
            "date_resigned": {
                "schema": {"data_type": "TIMESTAMP"},
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
            "archived": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates if the entity is archived",
                    "long_name": "Archived",
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
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date the transaction occurred",
                    "long_name": "Transaction Date",
                    "unit": "n/a"
                }
            },
            "nominal_value": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Nominal value of the shares",
                    "long_name": "Nominal Value",
                    "unit": "Currency"
                }
            },
            "amount_of_transaction": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of shares in the transaction",
                    "long_name": "Transaction Amount",
                    "unit": "count"
                }
            },
            "value_of_transaction": {
                "schema": {"data_type": "DOUBLE"},
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