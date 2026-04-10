column_definitions = {
    "aum_fund": {
        "table": {
            "comment": "Assets Under Management (AUM) reporting data at the fund level."
        },
        "columns": {
            "DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of capital available for investment but not yet deployed",
                    "long_name": "Dry Powder",
                    "unit": "fund currency"
                }
            },
            "EXPIRED_COMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital commitments that have expired",
                    "long_name": "Fund Expired Commitments",
                    "unit": "fund currency"
                }
            },
            "FUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Funds Under Management, the total amount of assets managed",
                    "long_name": "Funds Under Management",
                    "unit": "fund currency"
                }
            },
            "FUND_AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Assets Under Management for the fund",
                    "long_name": "Fund AUM",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_UP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of cash in the fund that has been allocated for investments - Fund/HoldCo Cash (Upstream)",
                    "long_name": "Fund Cash Upstream",
                    "unit": "fund currency"
                }
            },
            "FUND_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Gross Asset Value, total market value of all assets in the fund",
                    "long_name": "Fund GAV",
                    "unit": "fund currency"
                }
            },
            "FUND_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net Asset Value, the value of the fund’s assets minus its liabilities",
                    "long_name": "Fund NAV",
                    "unit": "fund currency"
                }
            },
            "FUND_SHORT": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund short name",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "REPORT_DATE": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date of the report for the fund",
                    "long_name": "Report Date",
                    "unit": "n/a"
                }
            },
            "TOTAL_CALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total capital called from investors",
                    "long_name": "Total Called Subscriptions",
                    "unit": "fund currency"
                }
            },
            "TOTAL_COMMITTED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total amount of capital committed by investors",
                    "long_name": "Fund Commitments",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_CAPITAL_HR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total capital of the fund held in reserves or for future use",
                    "long_name": "Total Fund Capital Held Reserves",
                    "unit": "fund currency"
                }
            },
            "TOTAL_RECALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total capital recalled from investors",
                    "long_name": "Total Recalled Capital",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_DOWN": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of cash drawn down from the fund - Fund/HoldCo Cash (Downstream)",
                    "long_name": "Fund Cash Downstream",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_EXP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Cash expenses related to the fund - Fund/HoldCo Expenses",
                    "long_name": "Fund Cash Expenses",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total debt associated with the fund",
                    "long_name": "Fund Drawn RCF/Fund Level Debt",
                    "unit": "fund currency"
                }
            },
            "UNDRAWN_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Debt that has been authorised but not yet drawn down",
                    "long_name": "Undrawn Fund RCF/Fund Level Debt",
                    "unit": "fund currency"
                }
            },
            "FUND_CAPITAL_DR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital drawn from investors for investments - New Commitments/Equity",
                    "long_name": "Fund Capital Drawn",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_OTHER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Other cash activities related to the fund - Fund/HoldCo Cash Other",
                    "long_name": "Fund Cash Other",
                    "unit": "fund currency"
                }
            },
            "FUND_CONDITIONAL_ACQ_EQ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital set aside for potential future acquisitions or investments",
                    "long_name": "Conditional Acquisitions (Equity)",
                    "unit": "fund currency"
                }
            },
            "FUND_REMOVE_DBL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of capital written off or removed from the fund",
                    "long_name": "Fund Remove Double Count",
                    "unit": "fund currency"
                }
            },
            "FUND_VALUE_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Adjustment made to the value of the fund assets - Other Fund Level Adjustment",
                    "long_name": "Fund Value Adjustment",
                    "unit": "fund currency"
                }
            },
            "RETURN_OF_CALL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Returns received from calls made to investors",
                    "long_name": "Returned of Called Subscriptions",
                    "unit": "fund currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "aum_asset": {
        "table": {
            "comment": "Assets Under Management (AUM) reporting data at the asset level."
        },
        "columns": {
            "ASSET_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total debt directly associated with the asset rather than to the company as a whole",
                    "long_name": "Asset Level Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount of capital available but not yet deployed for the asset",
                    "long_name": "Asset Dry Powder",
                    "unit": "n/a"
                }
            },
            "ASSET_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net Asset Value of the asset, calculated as the total value of assets minus liabilities",
                    "long_name": "Asset NAV",
                    "unit": "n/a"
                }
            },
            "AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total assets under management for the asset (Fund GAV + Asset-Level Committed Equity/Debt)",
                    "long_name": "Assets Under Management",
                    "unit": "n/a"
                }
            },
            "COMPANY_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the company associated with the asset",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "EQ_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Valuation of the equity portion of the asset, excluding cash",
                    "long_name": "Asset Equity Value",
                    "unit": "n/a"
                }
            },
            "FUND_SHORT": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund short name",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "LN_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Valuation of the loan or debt portion of the asset",
                    "long_name": "Asset Shareholder Loan Value",
                    "unit": "n/a"
                }
            },
            "PORTFOLIO_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Gross Asset Value of the portfolio to which the asset belongs (Asset NAV + Asset Level Loans)",
                    "long_name": "Portfolio GAV",
                    "unit": "n/a"
                }
            },
            "PORTFOLIO_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the portfolio the asset belongs to",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "REPORT_DATE": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date of the report for the asset",
                    "long_name": "Report Date",
                    "unit": "n/a"
                }
            },
            "EXTERNAL_DEBT_COMMIT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Commitment to external debt associated with the asset",
                    "long_name": "External Asset Debt Commitment",
                    "unit": "n/a"
                }
            },
            "LOAN_BALANCE": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Outstanding loan balance associated with the asset",
                    "long_name": "Asset Loan Balance",
                    "unit": "n/a"
                }
            },
            "ADJ_UNDRAWNCOMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Adjusted undrawn capital commitment for the asset",
                    "long_name": "Asset Commitment Adjustment",
                    "unit": "n/a"
                }
            },
            "ASSET_EQUITY_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Equity commitment for the asset",
                    "long_name": "Asset Committed Equity",
                    "unit": "n/a"
                }
            },
            "ASSET_DEBT_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Debt commitment for the asset",
                    "long_name": "Asset Committed Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Cash at the asset level",
                    "long_name": "Asset Level Cash",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Adjusted cash at the asset level",
                    "long_name": "Asset Level Cash Adjusted",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company": {
        "table": {
            "comment": "Master data table containing structural, geographic, and financial metadata for investee companies and SPVs."
        },
        "columns": {
            "AUM_Reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the company is included in Assets Under Management reporting (e.g., True/False).",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "Accounts_Due": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month or reporting period when the company’s accounts are due (e.g., March, September).",
                    "long_name": "Accounts Due Month",
                    "unit": "n/a"
                }
            },
            "Address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary registered address of the company.",
                    "long_name": "Company Address Line 1",
                    "unit": "n/a"
                }
            },
            "Address2": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Secondary registered address or suite details.",
                    "long_name": "Company Address Line 2",
                    "unit": "n/a"
                }
            },
            "Asset_Class": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The asset class to which the company belongs (e.g., Developer, Asset).",
                    "long_name": "Asset Class",
                    "unit": "n/a"
                }
            },
            "City": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The city where the company is registered or headquartered.",
                    "long_name": "City",
                    "unit": "n/a"
                }
            },
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full registered name of the company or SPV.",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "Company_ID": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal human-readable identifier for the company (e.g., COMP_1668).",
                    "long_name": "Internal Company ID",
                    "unit": "n/a"
                }
            },
            "Company_Number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Official government or state registration number of the company.",
                    "long_name": "Company Registration Number",
                    "unit": "n/a"
                }
            },
            "Company_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Structural classification of the company (e.g., Portfolio Holding Company, SPV).",
                    "long_name": "Company Type",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the company from the eFront API.",
                    "long_name": "eFront Company GUID",
                    "unit": "n/a"
                }
            },
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country where the company is currently registered.",
                    "long_name": "Registered Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the company record was originally created in the source system.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The base currency used by the company for financial reporting (e.g., GBP).",
                    "long_name": "Base Currency",
                    "unit": "n/a"
                }
            },
            "Financial_year_end": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month marking the end of the company’s financial year (e.g., June, December).",
                    "long_name": "Financial Year End Month",
                    "unit": "n/a"
                }
            },
            "House_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "House or building number associated with the company's registered address.",
                    "long_name": "House Number",
                    "unit": "n/a"
                }
            },
            "Known_as": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative alias, abbreviation, or common trading name for the company.",
                    "long_name": "Trading Name / Alias",
                    "unit": "n/a"
                }
            },
            "LEI": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The global Legal Entity Identifier (LEI) code for the company.",
                    "long_name": "Legal Entity Identifier",
                    "unit": "n/a"
                }
            },
            "Legal_Form": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The designated legal structure of the entity (e.g., GBR - Ltd - Limited).",
                    "long_name": "Legal Form",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the company record in eFront.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last time the company record was updated in the source system.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative or legacy reference number for the company (often mirrors Company_ID).",
                    "long_name": "Alternative Reference Number",
                    "unit": "n/a"
                }
            },
            "Original_country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The original country of registration if the company has relocated its domicile.",
                    "long_name": "Original Domicile Country",
                    "unit": "n/a"
                }
            },
            "Other_names": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Any other historical or associated names used by the company.",
                    "long_name": "Other Associated Names",
                    "unit": "n/a"
                }
            },
            "Site_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The internal system site or division the company belongs to (e.g., Business).",
                    "long_name": "System Site Name",
                    "unit": "n/a"
                }
            },
            "Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The current operational or management status of the company (e.g., SPV: Managed Subsidiary).",
                    "long_name": "Operational Status",
                    "unit": "n/a"
                }
            },
            "Tax_Due": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month or period when the company’s tax filings are due.",
                    "long_name": "Tax Due Month",
                    "unit": "n/a"
                }
            },
            "Technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary technology or operational sector of the company (e.g., Rooftop solar).",
                    "long_name": "Operating Technology",
                    "unit": "n/a"
                }
            },
            "VAT_Registration_No": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The company's official Value Added Tax (VAT) registration number.",
                    "long_name": "VAT Registration Number",
                    "unit": "n/a"
                }
            },
            "ZIP_Code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The postal or ZIP code for the company's registered address.",
                    "long_name": "Postal Code",
                    "unit": "n/a"
                }
            },
            "Lockbox_Date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The date associated with the company's lockbox or financial escrow mechanisms.",
                    "long_name": "Lockbox Date",
                    "unit": "n/a"
                }
            },
            "TIN": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Tax Identification Number for the company.",
                    "long_name": "Tax Identification Number",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "datapointsexport": {
        "table": {
            "comment": "Export table for individual data points."
        },
        "columns": {
            "CATEGORY_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Category name associated with the data point",
                    "long_name": "Data Category",
                    "unit": "n/a"
                }
            },
            "CREATIONDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the data point was created",
                    "long_name": "Data Point Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "CURRENCY": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used for the data point",
                    "long_name": "Currency Type",
                    "unit": "n/a"
                }
            },
            "DATAPOINT_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the data point",
                    "long_name": "Data Point Name",
                    "unit": "n/a"
                }
            },
            "ENTITY": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier for the entity associated with the data point",
                    "long_name": "Entity ID",
                    "unit": "n/a"
                }
            },
            "ENTITY_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the entity associated with the data point",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "ENTITY_TYPE": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of the entity (e.g., company, fund, etc.)",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "FILENAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the file containing the data point",
                    "long_name": "File Name",
                    "unit": "n/a"
                }
            },
            "MODIFICATIONDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the data point was last modified",
                    "long_name": "Data Point Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "PERIOD": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Period associated with the data point",
                    "long_name": "Reporting Period",
                    "unit": "n/a"
                }
            },
            "REFDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Reference date for the data point",
                    "long_name": "Reference Date",
                    "unit": "n/a"
                }
            },
            "REGION": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Geographic region related to the data point",
                    "long_name": "Region Name",
                    "unit": "n/a"
                }
            },
            "REPORTINGDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date on which the data point was reported",
                    "long_name": "Reporting Date",
                    "unit": "n/a"
                }
            },
            "REVISIONNUMBER": {
                "schema": {"data_type": "INT"},
                "description": {
                    "comment": "Revision number of the data point",
                    "long_name": "Revision Number",
                    "unit": "n/a"
                }
            },
            "SCENARIO": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Scenario associated with the data point (e.g., actual, forecast)",
                    "long_name": "Scenario Type",
                    "unit": "n/a"
                }
            },
            "TEMPLATE_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Template name associated with the data point",
                    "long_name": "Template Name",
                    "unit": "n/a"
                }
            },
            "VALUEMEMO": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Memo or notes describing the value of the data point",
                    "long_name": "Value Memo",
                    "unit": "n/a"
                }
            },
            "VALUENUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Numerical value of the data point",
                    "long_name": "Data Point Value",
                    "unit": "n/a"
                }
            },
            "VALUESTRING": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "String value representing the data point",
                    "long_name": "String Data Point Value",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fund": {
        "table": {
            "comment": "Master data table containing structural, classification, and lifecycle metadata for investment funds."
        },
        "columns": {
            "AIF": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the fund is classified as an Alternative Investment Fund (AIF) under AIFMD (e.g., Yes/No).",
                    "long_name": "Alternative Investment Fund Flag",
                    "unit": "n/a"
                }
            },
            "Closing_Date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The date when the fund was officially closed to new investors or finalized its initial fundraising.",
                    "long_name": "Fund Closing Date",
                    "unit": "n/a"
                }
            },
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country where the fund is registered or domiciled (e.g., United Kingdom).",
                    "long_name": "Fund Domicile Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the fund record was originally created in the source system.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A textual description of the fund, detailing its investment strategy, mandate, or focus.",
                    "long_name": "Fund Description",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full, official registered name of the fund.",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Fund_ID": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal human-readable identifier for the fund (e.g., FUND_1002).",
                    "long_name": "Internal Fund ID",
                    "unit": "n/a"
                }
            },
            "Fund_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fund from the eFront API.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "Legal_Form": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legally designated structure of the fund (e.g., GBR - PLC - Public Limited Company).",
                    "long_name": "Fund Legal Form",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the fund record in eFront.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last time the fund record was updated in the source system.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative or legacy reference number for the fund.",
                    "long_name": "Alternative Fund Number",
                    "unit": "n/a"
                }
            },
            "OEGEN_Fund_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal classification of the fund's lifecycle or structural status (e.g., Close-Ended, Fully invested).",
                    "long_name": "OEGEN Fund Classification",
                    "unit": "n/a"
                }
            },
            "Short_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The abbreviated name or ticker used internally to identify the fund (e.g., VCT, AEIT).",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Vintage_Year": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The vintage year the fund was launched or began making investments, used for benchmarking.",
                    "long_name": "Fund Vintage Year",
                    "unit": "year"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations": {
        "table": {
            "comment": "Data table for fund operations and associated activities."
        },
        "columns": {
            "Amount__fund_curr_": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount related to the fund operation in fund currency",
                    "long_name": "Fund Operation Amount",
                    "unit": "fund currency"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the fund operations record was created",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency in which the operation is denominated",
                    "long_name": "Operation Currency",
                    "unit": "n/a"
                }
            },
            "Effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date on which the fund operation takes effect",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the fund associated with the operation",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Fund_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Fund identifier from eFront API",
                    "long_name": "Fund ID",
                    "unit": "n/a"
                }
            },
            "Investor_Account": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor account related to the fund operation",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the fund operations record",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the fund operations record was last modified",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Share": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The class of share",
                    "long_name": "Share Class",
                    "unit": "n/a"
                }
            },
            "Short_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name or identifier for the fund operation",
                    "long_name": "Fund Operation Short Name",
                    "unit": "n/a"
                }
            },
            "Transaction_Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Unique identifier linking the transaction and the investor",
                    "long_name": "Transaction-Investor ID",
                    "unit": "n/a"
                }
            },
            "Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of fund operation (e.g., investment, fee payment)",
                    "long_name": "Fund Operation Type",
                    "unit": "n/a"
                }
            },
            "Valuation__fund_curr_": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Valuation of the investment in fund currency",
                    "long_name": "Fund Valuation",
                    "unit": "fund currency"
                }
            },
            "Committed_amount__fund_curr_": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total committed amount for the investment in fund currency",
                    "long_name": "Committed Amount",
                    "unit": "fund currency"
                }
            },
            "Call_Investments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for investments from investors",
                    "long_name": "Called Investment Amount",
                    "unit": "fund currency"
                }
            },
            "_Unit_Price": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Price per unit for the investment",
                    "long_name": "Unit Price",
                    "unit": "fund currency"
                }
            },
            "Call_Management_Fees_in_commitment": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called specifically for management fees",
                    "long_name": "Called Management Fees",
                    "unit": "fund currency"
                }
            },
            "Quantity": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Quantity of units associated with the fund operation",
                    "long_name": "Investment Quantity",
                    "unit": "n/a"
                }
            },
            "Redrawable_amount__fund_curr_": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Amount that can be redrawn in fund currency",
                    "long_name": "Redrawable Amount",
                    "unit": "fund currency"
                }
            },
            "FundExpired_Original_Commitment": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Original commitment amount that has expired",
                    "long_name": "Expired Original Commitment",
                    "unit": "fund currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "Draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Draft status of the record",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            }
        }
    },

    "fx": {
        "table": {
            "comment": "Foreign exchange rate data used for currency conversions."
        },
        "columns": {
            "Currency_Table": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifies the specific exchange rate table or source utilized (e.g., DEFAULT).",
                    "long_name": "Currency Table",
                    "unit": "n/a"
                }
            },
            "Destination_Curr": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The target currency into which the source currency is converted.",
                    "long_name": "Destination Currency",
                    "unit": "n/a"
                }
            },
            "Fx_Rate": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The exchange rate multiplier applied to convert the source currency to the destination currency.",
                    "long_name": "FX Rate",
                    "unit": "rate"
                }
            },
            "Ref_Date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The reference date and time that the exchange rate applies to.",
                    "long_name": "Reference Date",
                    "unit": "n/a"
                }
            },
            "Source_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The base currency being converted from.",
                    "long_name": "Source Currency",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "investoraccount": {
        "table": {
            "comment": "Data table for investor accounts."
        },
        "columns": {
            "AML_Compliance_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Anti-money laundering compliance status of the investor",
                    "long_name": "AML Compliance Status",
                    "unit": "n/a"
                }
            },
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the investor is located",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor account record was created",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used by the investor",
                    "long_name": "Currency Type",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the fund associated with the investor account",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Fund_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Fund identifier from eFront API",
                    "long_name": "Fund ID",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the investor",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "Investor_Account": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Account identifier for the investor",
                    "long_name": "Investor Account ID",
                    "unit": "n/a"
                }
            },
            "Investor_Account_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor account identifier from eFront API",
                    "long_name": "Investor Account ID",
                    "unit": "n/a"
                }
            },
            "Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor identifier from eFront API",
                    "long_name": "Investor ID",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of investor (e.g., individual, institutional)",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Legal_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Legal name of the investor",
                    "long_name": "Investor Legal Name",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the investor account record",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor account record was last modified",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "PEP_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Politically Exposed Person status of the investor",
                    "long_name": "PEP Status",
                    "unit": "n/a"
                }
            },
            "Risk_Level": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Risk level associated with the investor",
                    "long_name": "Investor Risk Level",
                    "unit": "n/a"
                }
            },
            "Short_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short identifier or code for the investor",
                    "long_name": "Investor Short Name",
                    "unit": "n/a"
                }
            },
            "Tax_Domicile__Country_": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the investor is domiciled for tax purposes",
                    "long_name": "Investor Tax Domicile Country",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "instruments": {
        "table": {
            "comment": "Data table for financial instruments."
        },
        "columns": {
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company associated with the instrument",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier from eFront API",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "Complete_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Complete name or description of the instrument",
                    "long_name": "Instrument Complete Name",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the instrument record was created",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency in which the instrument is denominated",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "Description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Description or details about the instrument",
                    "long_name": "Instrument Description",
                    "unit": "n/a"
                }
            },
            "Instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type or identifier of the instrument (e.g., stock, bond)",
                    "long_name": "Instrument Type",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the instruments record",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the instruments record was last modified",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "OEGEN_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of OEGEN (Operational Entity Generation) associated with the instrument",
                    "long_name": "OEGEN Type",
                    "unit": "n/a"
                }
            },
            "Portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Portfolio to which the instrument belongs",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "investor": {
        "table": {
            "comment": "Master data table for investors."
        },
        "columns": {
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the company is registered",
                    "long_name": "Company Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor record was created",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used by the company for financial reporting",
                    "long_name": "Company Currency",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the investor",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used by the investee company",
                    "long_name": "Investee Currency",
                    "unit": "n/a"
                }
            },
            "Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor identifier from eFront API",
                    "long_name": "Investor ID",
                    "unit": "n/a"
                }
            },
            "Legal_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Legal name of the investor",
                    "long_name": "Investor Legal Name",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the investor record",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor record was last modified",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Risk_Level": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Risk level associated with the investor",
                    "long_name": "Investor Risk Level",
                    "unit": "n/a"
                }
            },
            "Tax_Domicile__Country_": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the investor is domiciled for tax purposes",
                    "long_name": "Investor Tax Domicile Country",
                    "unit": "n/a"
                }
            },
            "AML_Compliance_Status": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date of the first investment in the investee",
                    "long_name": "First Investment Date",
                    "unit": "n/a"
                }
            },
            "PEP_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Politically Exposed Person status of the investor",
                    "long_name": "PEP Status",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "ownership": {
        "table": {
            "comment": "Data table mapping ownership stakes, investment relationships, and holding structures between entities."
        },
        "columns": {
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full registered name of the investee company or asset being held.",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investee company from eFront.",
                    "long_name": "Investee Company GUID",
                    "unit": "n/a"
                }
            },
            "Company_Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investing company, SPV, or intermediate holding entity.",
                    "long_name": "Investor Company Name",
                    "unit": "n/a"
                }
            },
            "Company_Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investing company from eFront.",
                    "long_name": "Investor Company GUID",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full name of the fund sitting at the top of this ownership relationship.",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Fund_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fund from eFront.",
                    "long_name": "Fund GUID",
                    "unit": "n/a"
                }
            },
            "Investee_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary currency used by the investee company (e.g., GBP, USD).",
                    "long_name": "Investee Currency",
                    "unit": "n/a"
                }
            },
            "Investment_Details": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name, grouping, or categorical details identifying the investment (e.g., Fern, ORIT).",
                    "long_name": "Investment Details / Grouping",
                    "unit": "n/a"
                }
            },
            "Investor_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary currency used by the investing entity.",
                    "long_name": "Investor Currency",
                    "unit": "n/a"
                }
            },
            "AUM_Reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether this specific investment relationship is included in Assets Under Management reporting.",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "perc_Stake": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of ownership or equity stake held by the investor in the investee.",
                    "long_name": "Ownership Stake Percentage",
                    "unit": "%"
                }
            },
            "First_Investment_Date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact date when the initial investment or acquisition was made into the investee company.",
                    "long_name": "First Investment Date",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "track_record_asset": {
        "table": {
            "comment": "Track record performance metrics at the asset level."
        },
        "columns": {
            "ANNUALISED_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annualised yield for the asset",
                    "long_name": "Annualised Yield",
                    "unit": "%"
                }
            },
            "ANNUAL_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annual yield for the asset",
                    "long_name": "Annual Yield",
                    "unit": "%"
                }
            },
            "FUND_SHORT": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name of the fund associated with the asset",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "IRR_FROM_TODAY": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Internal rate of return from today for the asset",
                    "long_name": "IRR From Today",
                    "unit": "%"
                }
            },
            "LEVERAGE": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Leverage for the asset",
                    "long_name": "Leverage",
                    "unit": "n/a"
                }
            },
            "LONG_TERM_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Long-term internal rate of return for the asset",
                    "long_name": "Long Term IRR",
                    "unit": "%"
                }
            },
            "MOIC": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Multiple on invested capital for the asset",
                    "long_name": "MOIC",
                    "unit": "n/a"
                }
            },
            "NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net asset value for the asset",
                    "long_name": "NAV",
                    "unit": "n/a"
                }
            },
            "PORTFOLIO_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the portfolio associated with the asset",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "QUARTERLY_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Quarterly yield for the asset",
                    "long_name": "Quarterly Yield",
                    "unit": "%"
                }
            },
            "REPORTING_DATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date of the report for the asset",
                    "long_name": "Reporting Date",
                    "unit": "n/a"
                }
            },
            "UNREALISED_NET_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unrealised net internal rate of return for the asset",
                    "long_name": "Unrealised Net IRR",
                    "unit": "%"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "track_record_fund": {
        "table": {
            "comment": "Track record performance metrics at the fund level."
        },
        "columns": {
            "ANNUALISED_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annualised yield for the fund",
                    "long_name": "Annualised Yield",
                    "unit": "%"
                }
            },
            "ANNUAL_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annual yield for the fund",
                    "long_name": "Annual Yield",
                    "unit": "%"
                }
            },
            "DPI": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Distributions to Paid-In Capital ratio",
                    "long_name": "DPI",
                    "unit": "n/a"
                }
            },
            "FUND_SHORT": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name of the fund",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "MOIC": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Multiple on invested capital for the fund",
                    "long_name": "MOIC",
                    "unit": "n/a"
                }
            },
            "NET_CAGR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net compound annual growth rate for the fund",
                    "long_name": "Net CAGR",
                    "unit": "%"
                }
            },
            "NET_TOTAL_RETURN": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net total return of the fund",
                    "long_name": "Net Total Return",
                    "unit": "%"
                }
            },
            "REPORTING_DATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date of the report for the fund",
                    "long_name": "Reporting Date",
                    "unit": "n/a"
                }
            },
            "UNREALISED_GROSS_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unrealised gross internal rate of return for the fund",
                    "long_name": "Unrealised Gross IRR",
                    "unit": "%"
                }
            },
            "UNREALISED_NET_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unrealised net internal rate of return for the fund",
                    "long_name": "Unrealised Net IRR",
                    "unit": "%"
                }
            },
            "WEIGHTED_YIELD_UNITS": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Weighted units calculation used for yield analysis",
                    "long_name": "Weighted Yield Units",
                    "unit": "n/a"
                }
            },
            "ANNUAL_NET_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annual net internal rate of return for the fund",
                    "long_name": "Annual Net IRR",
                    "unit": "%"
                }
            },
            "NAV_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net asset value yield for the fund",
                    "long_name": "NAV Yield",
                    "unit": "%"
                }
            },
            "QUARTERLY_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Quarterly yield for the fund",
                    "long_name": "Quarterly Yield",
                    "unit": "%"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "transactions": {
        "table": {
            "comment": "Data table containing granular, itemized financial transaction records (e.g., loans, advances, repayments)."
        },
        "columns": {
            "AUM_Reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the transaction is included in Assets Under Management reporting (e.g., True/False).",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "Amount": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total financial amount of the transaction in the base or reporting currency.",
                    "long_name": "Transaction Amount (Base)",
                    "unit": "base currency"
                }
            },
            "Amount_-_Instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The financial amount of the transaction expressed in the instrument's local currency.",
                    "long_name": "Instrument Amount (Local)",
                    "unit": "instrument currency"
                }
            },
            "Commitment": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total capital commitment associated with this transaction or instrument line.",
                    "long_name": "Transaction Commitment (Base)",
                    "unit": "base currency"
                }
            },
            "Commitment_-_Instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total capital commitment expressed in the instrument's local currency.",
                    "long_name": "Instrument Commitment (Local)",
                    "unit": "instrument currency"
                }
            },
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investee company or entity involved in the transaction.",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investee company.",
                    "long_name": "Investee Company GUID",
                    "unit": "n/a"
                }
            },
            "Company_Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investor entity, fund, or SPV executing the transaction.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "Company_Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investor entity.",
                    "long_name": "Investor GUID",
                    "unit": "n/a"
                }
            },
            "Complete_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The overarching classification or name of the transaction (e.g., Shareholder Loan).",
                    "long_name": "Transaction Name",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the transaction record was created in the source system.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A granular description of the transaction's purpose (e.g., Credit Facility Loan).",
                    "long_name": "Transaction Description",
                    "unit": "n/a"
                }
            },
            "Draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the transaction is in a draft or uncommitted state (e.g., True/False).",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            },
            "Effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The actual effective or execution date of the transaction.",
                    "long_name": "Transaction Effective Date",
                    "unit": "n/a"
                }
            },
            "Exchange_Rate_Instr_->_Investor": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The specific FX exchange rate applied to convert the instrument currency to the investor currency.",
                    "long_name": "Applied Exchange Rate",
                    "unit": "rate"
                }
            },
            "Exclude_transaction_costs": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating whether transaction costs are excluded from the calculated amounts.",
                    "long_name": "Exclude Transaction Costs Flag",
                    "unit": "n/a"
                }
            },
            "Index": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The sequential index or ordering number of the transaction.",
                    "long_name": "Transaction Index",
                    "unit": "n/a"
                }
            },
            "Instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific financial instrument utilized (e.g., Shareholder Loan, Equity).",
                    "long_name": "Transaction Instrument",
                    "unit": "n/a"
                }
            },
            "Instrument_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The local currency in which the instrument is denominated (e.g., EUR, GBP).",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "Investment_Details": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The grouping or portfolio segment associated with the transaction (e.g., ORIP).",
                    "long_name": "Investment Grouping Details",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the transaction record.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the transaction in the source system.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific project or portfolio name the transaction relates to (e.g., Gaishecke).",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "Transaction_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the individual transaction.",
                    "long_name": "Transaction GUID",
                    "unit": "n/a"
                }
            },
            "Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The exact action or accounting type of the transaction (e.g., LN - Repayment, LN - Advance).",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "Valuation": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The assessed valuation of the transaction/instrument in the base currency.",
                    "long_name": "Transaction Valuation (Base)",
                    "unit": "base currency"
                }
            },
            "Valuation_-_Instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The assessed valuation of the transaction/instrument in the local instrument currency.",
                    "long_name": "Instrument Valuation (Local)",
                    "unit": "instrument currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            }
        }
    }
}