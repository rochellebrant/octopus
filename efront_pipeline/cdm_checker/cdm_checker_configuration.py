config = {


    "summary_table": {
        "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            }
    },


############################################################################################################


    "aum_funds":
        {
            "input_method": {
                "CDM": "sql_query",
                "Data": "sql_query"
                },
            "sql_queries":  { "Data": "efront_funds_dim",
                              "CDM":  "cdm_funds" },
            "join_type": "left",
            "join_columns": { "primary": [ {"CDM": "fund_core_id", "Data": "MAPPED_CDM_FUND_ID"} ],
                              "secondary": [ {"CDM": "fund_display_name", "Data": "EFRONT_FUND_SHORT"},
                                             {"CDM": "fund_legal_name", "Data": "EFRONT_FUND_NAME"} ]
            },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            }
        },



    "aum_companies": 
        {
            "input_method": {
                "CDM": "sql_query",
                "Data": "sql_query"
                },
            "sql_queries":  { "Data": "efront_companies_aum_fact",
                             "CDM":  "cdm_companies" },
            "join_type": "left",
            "join_columns": { "primary": [ {"CDM": "company_core_id", "Data": "MAPPED_CDM_COMPANY_ID"} ],
                              "secondary": [ {"CDM": "company_registered_name", "Data": "EFRONT_COMPANY_NAME"},
                                            {"CDM": "company_type_name", "Data": "Company_Type"},
                                            {"CDM": "company_registered_incorporation_number", "Data": "Company_Number"},
                                            {"CDM": "registered_office_address", "Data": "Address"},
                                            {"CDM": "vat_number", "Data": "VAT_Registration_No"},
                                            {"CDM": "technology_name", "Data": "Technology"},
                                            {"CDM": "country_name", "Data": "Country"}
                                            ]
                            },
            "timestamp_formats": { "Created_on": "M/d/yyyy h:mm:ss a" },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            },
        },



    "aum_portfolios":
        {
            "input_method": {
                "CDM": "table",
                "Data": "sql_query"
                },
            "tables":       { "CDM": "oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core" },
            "sql_queries":  { "Data": "efront_portfolios_aum_fact" },
            "join_type": "left",
            "join_columns": { "primary": [ {"CDM": "investment_portfolio_name", "Data": "PORTFOLIO_NAME"} ]
                            },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            }
        },


############################################################################################################
    

    "raw_funds":
        {
            "input_method": {
                "CDM": "sql_query",
                "Data": "sql_query"
                },
            "sql_queries": { "Data": "efront_funds_dim",
                             "CDM":  "cdm_funds" },
            "join_columns": {
                "primary": [
                    {"CDM": "fund_core_id", "Data": "MAPPED_CDM_FUND_ID"}
                    ],
                "secondary": [
                    {"CDM": "fund_legal_name", "Data": "EFRONT_FUND_NAME"},
                    {"CDM": "fund_display_name", "Data": "EFRONT_FUND_SHORT"}
                    ]
            },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            }
        },



    "raw_companies": 
        {
            "input_method": {
                "CDM": "sql_query",
                "Data": "sql_query"
                },
            "sql_queries":  { "Data": "efront_companies_dim",
                              "CDM":  "cdm_companies" },
            "join_columns": { "primary": [ {"CDM": "company_core_id", "Data": "MAPPED_CDM_COMPANY_ID"} ],
                              "secondary": [ {"CDM": "company_registered_name", "Data": "EFRONT_COMPANY_NAME"},
                                            {"CDM": "company_type_name", "Data": "Company_Type"},
                                            {"CDM": "company_registered_incorporation_number", "Data": "Company_Number"},
                                            {"CDM": "registered_office_address", "Data": "Address"},
                                            {"CDM": "vat_number", "Data": "VAT_Registration_No"},
                                            {"CDM": "technology_name", "Data": "Technology"},
                                            {"CDM": "country_name", "Data": "Country"}
                                            ]
                            },
            "timestamp_formats": {
                "Created_on": "M/d/yyyy h:mm:ss a"
                },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            },
        },


    
    "raw_portfolios":
        {
            "input_method": {
                "CDM": "table",
                "Data": "sql_query"
                },
            "tables": {
                "CDM": "oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core",
            },
            "sql_queries": {
                "Data": "efront_portfolios_dim"
            },
            "join_columns": {
                "primary": [
                    {"CDM": "investment_portfolio_id", "Data": "MAPPED_CDM_PORTFOLIO_ID"}
                    ]
            },
            "save_to": {
                "catalog": "oegen_data_prod_source",
                "database": "src_blackrock_efront_xio"
            }
        },
}