table_config = {
# dim
"company":
            {
                "scd2_enabled": False,
                "match_keys": ["Company_IQId"],
                "load_date_column": "Modified_on",
                "non_null_columns": [
                    "Accounts_Due", "Address", "Address2", "Asset_Class", "City", "Company", "Company_ID",
                    "Company_Number", "Company_Type", "Company_IQId", "Country", "Created_on", "Currency",
                    "Financial_year_end", "House_number", "Known_as", "LEI", "Legal_Form", "Modified_by",
                    "Modified_on", "Number", "Original_country", "Other_names", "Site_Name", "Status",
                    "Tax_Due", "Technology", "VAT_Registration_No", "ZIP_Code", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : ["Created_on", "Modified_by"],
                "timestamp_formats": {
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "individual" : ["Company_IQId", "Company"],
                    "composite": [["Company_IQId", "Company"]]
                }
            }, 
# fact            
"datapointsexport":
            {
                "scd2_enabled": False,
                "match_keys": ["CATEGORY_NAME", "DATAPOINT_NAME", "ENTITY", "ENTITY_NAME", "REPORTINGDATE"],
                "load_date_column": "REFDATE",
                "non_null_columns": [
                    "CATEGORY_NAME", "CREATIONDATE", "CURRENCY", "DATAPOINT_NAME", "ENTITY", "ENTITY_NAME",
                    "ENTITY_TYPE", "FILENAME", "MODIFICATIONDATE", "PERIOD", "REFDATE", "REGION",
                    "REPORTINGDATE", "REVISIONNUMBER", "SCENARIO", "TEMPLATE_NAME", "VALUEMEMO",
                    "VALUENUM", "VALUESTRING", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : [],
                "timestamp_formats": {
                    "CREATIONDATE": "M/d/yyyy h:mm:ss a",
                    "MODIFICATIONDATE": "M/d/yyyy h:mm:ss a",
                    "REFDATE": "M/d/yyyy h:mm:ss a",
                    "REPORTINGDATE": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "composite": [["CATEGORY_NAME", "CREATIONDATE", "DATAPOINT_NAME", "ENTITY", "REFDATE"]]
                }
            },
# dim
"fund":
            {
                "scd2_enabled": False,
                "match_keys": ["Fund_IQId"],
                "load_date_column": "Modified_on",
                "non_null_columns": [
                    "AIF", "Created_on", "Description", "Fund", "Fund_ID", "Fund_IQId",
                    "Legal_Form", "Modified_by", "Modified_on", "OEGEN_Fund_Type", "Short_Name", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : ["Created_on", "Modified_by"],
                "timestamp_formats": {
                    "Closing_Date": "M/d/yyyy h:mm:ss a",
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "individual" : ["Fund_IQId", "Fund", "Short_Name"],   # per-column checks
                    "composite": [["Fund_IQId", "Fund", "Short_Name"]]    # columns to be treated together (one or more combos)
                }
            },
# dim
"fundoperations":
            {
                "scd2_enabled": False,
                "match_keys": ["Transaction_Investor_IQId"], #  ["Fund_IQId", "Type", "Investor_Account"]
                "load_date_column": "Effective_date",
                "non_null_columns": [
                    "Created_on", "Currency", "Effective_date", "Fund", "Fund_IQId",
                    "Investor_Account", "Modified_by", "Modified_on", "Share", "Short_Name",
                    "Transaction_Investor_IQId", "Type", "datalake_ingestion_timestamp", "Draft"
                ],
                "audit_keys" : ["Created_on", "Modified_on", "Modified_by"],
                "timestamp_formats": {
                    "Effective_date": "M/d/yyyy h:mm:ss a",
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                # "uniqueness_expectations": {
                #     "individual" : ["Transaction_Investor_IQId"]
                # }
            },
# fx        
"fx":
            {
                "scd2_enabled": False,
                "match_keys": ["Destination_Curr", "Source_Currency", "Ref_Date"],
                "load_date_column": "Ref_Date",
                "non_null_columns": ["Destination_Curr", "Source_Currency", "Ref_Date", "Fx_Rate"],
                "timestamp_formats": {
                    "Ref_Date": "M/d/yyyy h:mm:ss a",
                    },
                "uniqueness_expectations": {
                    "composite": [["Destination_Curr", "Source_Currency", "Ref_Date"]]    # columns to be treated together (one or more combos)
                }
            },
# dim
"instruments":
            {
                "scd2_enabled": False,
                "match_keys": ["Company_IQId", "Instrument", "Portfolio", "Description", "Currency"],
                "load_date_column": "Modified_on",
                "non_null_columns": [
                    "Company", "Company_IQId", "Complete_Name", "Created_on", "Currency", "Description",
                    "Instrument", "Modified_by", "Modified_on", "OEGEN_type", "Portfolio", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : ["Created_on", "Modified_by"],
                "timestamp_formats": {
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "composite": [["Company_IQId", "Instrument", "Description", "Portfolio"]]
                }
            },
# dim
"investoraccount":
            {
                "scd2_enabled": False,
                "match_keys": ["Investor_Account_IQId"],
                "load_date_column": "Modified_on",
                "non_null_columns": [
                    "Country", "Currency", "Fund", "Fund_IQId", "Investor", "Investor_Account",
                    "Investor_Type", "Investor_Account_IQId", "Investor_IQId", "Legal_Name", "Modified_by",
                    "Risk_Level", "Short_Name", "Tax_Domicile__Country_", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : ["Created_on", "Modified_by"],
                "timestamp_formats": {
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "individual" : ["Investor_Account_IQId"]
                }
            },
# dim
"investor":
            {
                "scd2_enabled": False,
                "match_keys": ["Investor_IQId"],
                "load_date_column": "Modified_on",
                "non_null_columns": [
                    "Country", "Created_on", "Currency", "Investor", "Investor_Type", "Investor_IQId",
                    "Legal_Name", "Modified_by", "Modified_on", "Risk_Level", "Tax_Domicile__Country_", "datalake_ingestion_timestamp"
                ],
                "audit_keys" : ["Created_on", "Modified_by"],
                "timestamp_formats": {
                    "Created_on": "M/d/yyyy h:mm:ss a",
                    "Modified_on": "M/d/yyyy h:mm:ss a"
                    },
                "uniqueness_expectations": {
                    "individual" : ["Investor_IQId"]
                }
            },
# dim
"ownership": # This one doesn't have a meaningful timestamp for SCD 2...might never have one...?
            {
                "scd2_enabled": False,
                "match_keys": ["Company_IQId", "Fund_IQId", "Company_Investor_IQId"],
                "load_date_column": "datalake_ingestion_timestamp",
                "non_null_columns": [
                    "Company", "Company_IQId", "Fund", "Fund_IQId", "Investee_currency", "Investment_Details",
                    "Company_Investor", "Company_Investor_IQId", "Investor_currency", "datalake_ingestion_timestamp"
                ],
                "uniqueness_expectations": {
                    "composite": [["Company_IQId", "Fund_IQId", "Company_Investor_IQId"],]
                }
            },

# "track_record_asset":
#             {   
#                 "scd2_enabled": False,
#                 "match_keys": ["FUND_SHORT", "PORTFOLIO_NAME"],
#                 "load_date_column": "REPORTING_DATE",
#                 "non_null_columns": ["REPORTING_DATE", "FUND_SHORT", "PORTFOLIO_NAME", "ANNUALISED_YIELD",	"ANNUAL_YIELD",	"IRR_FROM_TODAY", "LONG_TERM_IRR", "NAV", "QUARTERLY_YIELD", "UNREALISED_NET_IRR"],
#                 "audit_keys" : [],
#                 "timestamp_formats": {
#                     "REPORTING_DATE": "M/d/yyyy h:mm:ss a"
#                     }
#             },

"track_record_fund":
            {
                "scd2_enabled": False,
                "match_keys": ["FUND_SHORT"],
                "load_date_column": "REPORTING_DATE",
                "non_null_columns": ["REPORTING_DATE", "FUND_SHORT"],
                "audit_keys" : [],
                "timestamp_formats": {
                    "REPORTING_DATE": "M/d/yyyy h:mm:ss a"
                    }
            },

# "track_record_investor":
#             {"match_keys": ["FUND_SHORT", "INVESTOR_NAME", "SUBSCRIBER_NAME"],
#             "load_date_column": "REPORTING_DATE",
#             "non_null_columns":["REPORTING_DATE", "INVESTOR_NAME", "SUBSCRIBER_NAME", "FUND_SHORT"],
#             "audit_keys" : [],
#             "timestamp_formats": {
#                 "REPORTING_DATE": "M/d/yyyy h:mm:ss a"
#                 }
#             },

# fact
"transactions":
        {   
            "scd2_enabled": False,
            "match_keys": ["Transaction_IQId"], # "Complete_Name", "Instrument", "Instrument_currency", "Type", "Investment_Details", "Company_Investor"
            "load_date_column": "Effective_date",
            "non_null_columns": [
                "Company", "Company_IQId", "Complete_Name", "Created_on", "Description",
                "Effective_date", "Transaction_IQId", "Instrument", "Instrument_currency", "Investment_Details", "Company_Investor", "Company_Investor_IQId", "Modified_by",
                "Modified_on", "Type", "datalake_ingestion_timestamp"
            ],
            "audit_keys" : ["Created_on", "Modified_on", "Modified_by", "Index"],
            "timestamp_formats": {
                "Created_on": "M/d/yyyy h:mm:ss a",
                "Effective_date": "M/d/yyyy h:mm:ss a",
                "Modified_on": "M/d/yyyy h:mm:ss a"
                },
            "uniqueness_expectations": {
                    "individual" : ["Transaction_IQId"]
                }
        },
            
}