table_config = {
    # dim
    "company": {
        "scd2_enabled": False,
        "match_keys": ["Company_IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    }, 

    # fact            
    "datapointsexport": {
        "scd2_enabled": False,
        "match_keys": ["CATEGORY_NAME", "DATAPOINT_NAME", "ENTITY", "ENTITY_NAME", "REPORTINGDATE"],
        "load_date_column": "REFDATE",
        "audit_keys": [],
        "timestamp_formats": {
            "CREATIONDATE": "M/d/yyyy h:mm:ss a",
            "MODIFICATIONDATE": "M/d/yyyy h:mm:ss a",
            "REFDATE": "M/d/yyyy h:mm:ss a",
            "REPORTINGDATE": "M/d/yyyy h:mm:ss a"
        },
    },

    # fact            
    "fees_company": {
        "scd2_enabled": False,
        "match_keys": ["IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Effective_date": "M/d/yyyy h:mm:ss a",
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # fact
    "fees_fund": {
        "scd2_enabled": False,
        "match_keys": ["IQId", "Investor_Account_IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Effective_date": "M/d/yyyy h:mm:ss a",
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "fund": {
        "scd2_enabled": False,
        "match_keys": ["Fund_IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Closing_Date": "M/d/yyyy h:mm:ss a",
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "fundoperations": {
        "scd2_enabled": False,
        "match_keys": ["Transaction_Investor_IQId"],
        "load_date_column": "Effective_date",
        "audit_keys": ["Created_on", "Modified_on", "Modified_by"],
        "timestamp_formats": {
            "Effective_date": "M/d/yyyy h:mm:ss a",
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # fx        
    "fx": {
        "scd2_enabled": False,
        "match_keys": ["Destination_Curr", "Source_Currency", "Ref_Date"],
        "load_date_column": "Ref_Date",
        "non_null_columns": ["Destination_Curr", "Source_Currency", "Ref_Date", "Fx_Rate"],
        "timestamp_formats": {
            "Ref_Date": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "instruments": {
        "scd2_enabled": False,
        "match_keys": ["Company_IQId", "Instrument", "Portfolio", "Description", "Currency"],
        "load_date_column": "Modified_on",
        "non_null_columns": [
            "Company", "Company_IQId", "Complete_Name", "Created_on", "Currency", "Description",
            "Instrument", "Modified_by", "Modified_on", "OEGEN_type", "Portfolio", "datalake_ingestion_timestamp"
        ],
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "investoraccount": {
        "scd2_enabled": False,
        "match_keys": ["Investor_Account_IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "investor": {
        "scd2_enabled": False,
        "match_keys": ["Investor_IQId"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },

    # dim
    "ownership": {
        "scd2_enabled": False,
        "match_keys": ["Company_IQId", "Fund_IQId", "Company_Investor_IQId"],
        "load_date_column": "datalake_ingestion_timestamp",
        "non_null_columns": [
            "Company", "Company_IQId", "Fund", "Fund_IQId", "Investee_currency", "Investment_Details",
            "Company_Investor", "Company_Investor_IQId", "Investor_currency", "datalake_ingestion_timestamp"
        ],
    },

    "track_record_fund": {
        "scd2_enabled": False,
        "match_keys": ["FUND_SHORT"],
        "load_date_column": "REPORTING_DATE",
        "non_null_columns": ["REPORTING_DATE", "FUND_SHORT"],
        "audit_keys": [],
        "timestamp_formats": {
            "REPORTING_DATE": "M/d/yyyy h:mm:ss a"
        }
    },

    # fact
    "transactions": {   
        "scd2_enabled": False,
        "match_keys": ["Transaction_IQId"], # "Complete_Name", "Instrument", "Instrument_currency", "Type", "Investment_Details", "Company_Investor"
        "load_date_column": "Effective_date",
        "non_null_columns": [
            "Company", "Company_IQId", "Created_on", "Description", "Effective_date", "Transaction_IQId", 
            "Instrument", "Instrument_currency", "Investment_Details", 
            "Modified_by", "Modified_on", "Type", "datalake_ingestion_timestamp"
        ],
        "audit_keys": ["Created_on", "Modified_on", "Modified_by", "Index"],
        "timestamp_formats": {
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Effective_date": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a"
        },
    },
    # fact            
    "transactions_paid": {
        "scd2_enabled": False,
        "match_keys": ["Transaction_IQId", "Date_Paid"],
        "load_date_column": "Modified_on",
        "audit_keys": ["Created_on", "Modified_by"],
        "timestamp_formats": {
            "Effective_date": "M/d/yyyy h:mm:ss a",
            "Created_on": "M/d/yyyy h:mm:ss a",
            "Modified_on": "M/d/yyyy h:mm:ss a",
            "Date_Paid": "M/d/yyyy h:mm:ss a"
        },
    }       
}