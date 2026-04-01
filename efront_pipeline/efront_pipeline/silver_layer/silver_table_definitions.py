silver_table_definitions = {
    "aum_bridge": {
        "table": {
            "comment": "Silver layer AUM waterfall table. Explains the delta between Previous AUM and Current AUM by attributing movements to Capital Injections, Disposals, Asset NAV changes, and Fund-level adjustments."
        },
        "columns": {
            "Report_Date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The reporting date for which the AUM movement is being calculated.",
                    "long_name": "Report Date",
                    "unit": "n/a"
                }
            },
            "Previous_AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total AUM value from the immediately preceding reporting period (Opening Balance).",
                    "long_name": "Previous AUM (Opening)",
                    "unit": "GBP"
                }
            },
            "Asset_Capital_Injections": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total capital deployed into assets during the period (Purchases, Subscriptions, and Advances). Sourced from the AUM transactions table.",
                    "long_name": "Asset Capital Injections",
                    "unit": "GBP"
                }
            },
            "Asset_NAV_Change": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The residual movement in AUM representing the change in market valuation of assets, net of capital movements and adjustments.",
                    "long_name": "Asset NAV Performance/Change",
                    "unit": "GBP"
                }
            },
            "Asset_Committed_Equity": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The period-on-period change in committed but undrawn equity, debt, and adjusted commitments at the asset level.",
                    "long_name": "Change in Asset Committed Equity",
                    "unit": "GBP"
                }
            },
            "Asset_Debt_Movements": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The period-on-period change in actual asset-level debt balances.",
                    "long_name": "Asset Debt Movements",
                    "unit": "GBP"
                }
            },
            "Asset_Disposals": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Total capital returned through asset sales or redemptions during the period. Sourced from the AUM transactions table.",
                    "long_name": "Asset Disposals",
                    "unit": "GBP"
                }
            },
            "Fund_Level_Adj": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The period-on-period change in fund-level cash, expenses, and double-count removals.",
                    "long_name": "Fund Level Adjustments Movement",
                    "unit": "GBP"
                }
            },
            "Current_AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total AUM value as of the current reporting date (Closing Balance).",
                    "long_name": "Current AUM (Closing)",
                    "unit": "GBP"
                }
            },
            "Refresh_Timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver bridge table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "aum_capital_injections_and_disposals_transactions": {
        "table": {
            "comment": "Silver layer transaction table containing capital injections (purchases, subscriptions, advances) and disposals (sales, redemptions). Transactions are converted to GBP and mapped to AUM reporting periods (buckets)."
        },
        "columns": {
            "transaction_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the individual transaction.",
                    "long_name": "Transaction GUID",
                    "unit": "n/a"
                }
            },
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The mapped universal identifier for the fund according to the Core Data Model.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "cdm_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The mapped universal identifier for the portfolio/asset according to the Core Data Model.",
                    "long_name": "CDM Portfolio ID",
                    "unit": "n/a"
                }
            },
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "ef_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the portfolio.",
                    "long_name": "eFront Portfolio GUID",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund name, including manual overrides for specific groupings like Sky or Vector.",
                    "long_name": "eFront Fund Name (Corrected)",
                    "unit": "n/a"
                }
            },
            "ef_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific project or portfolio name as recorded in eFront.",
                    "long_name": "eFront Portfolio Name",
                    "unit": "n/a"
                }
            },
            "investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investing entity or SPV that executed the transaction.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction type filtered for injections/disposals (e.g., EQ - Purchase, LN - Advance, EQ - Sale).",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "amount": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The financial amount of the transaction in its original fund currency.",
                    "long_name": "Original Amount",
                    "unit": "currency"
                }
            },
            "transaction_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The effective date of the financial transaction.",
                    "long_name": "Transaction Date",
                    "unit": "n/a"
                }
            },
            "transaction_month_end": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The last day of the month for the transaction date, used for FX alignment and period bucketing.",
                    "long_name": "Transaction Month End",
                    "unit": "n/a"
                }
            },
            "currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The ISO currency code for the original transaction amount.",
                    "long_name": "Source Currency",
                    "unit": "n/a"
                }
            },
            "fx_ref_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The reference date from the FX table used to convert this transaction to GBP.",
                    "long_name": "FX Reference Date",
                    "unit": "n/a"
                }
            },
            "used_rate_to_gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The specific exchange rate multiplier applied to convert the local amount to GBP.",
                    "long_name": "Applied FX Rate (to GBP)",
                    "unit": "rate"
                }
            },
            "amount_gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The transaction amount converted into British Pounds (GBP).",
                    "long_name": "Amount (GBP)",
                    "unit": "GBP"
                }
            },
            "tx_effective_report_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The AUM Report Date this transaction is bucketed into (where transaction_month_end falls between report periods).",
                    "long_name": "AUM Reporting Bucket Date",
                    "unit": "n/a"
                }
            },
            "tx_bucket_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the transaction successfully matched an AUM reporting period (bucketed_tx) or fell outside known periods (unbucketed_tx).",
                    "long_name": "Bucketing Status",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "aum_metrics": {
        "table": {
            "comment": "Silver layer table unifying and aggregating asset-level and fund-level AUM metrics by portfolio and reporting date. Includes derived NAV, GAV, and AUM calculations and validation checks."
        },
        "columns": {
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "ef_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the portfolio.",
                    "long_name": "eFront Portfolio GUID",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund short name from eFront.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "ef_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the portfolio from eFront.",
                    "long_name": "eFront Portfolio Name",
                    "unit": "n/a"
                }
            },
            "REPORT_DATE": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date of the report for the aggregated metrics.",
                    "long_name": "Report Date",
                    "unit": "n/a"
                }
            },
            "FUND_CASH_UP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash in the fund allocated for investments (Upstream).",
                    "long_name": "Aggregated Fund Cash Upstream",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_DOWN": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash drawn down from the fund (Downstream).",
                    "long_name": "Aggregated Fund Cash Downstream",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_EXP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash expenses related to the fund.",
                    "long_name": "Aggregated Fund Cash Expenses",
                    "unit": "fund currency"
                }
            },
            "FUND_CAPITAL_DR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital drawn from investors for investments.",
                    "long_name": "Aggregated Fund Capital Drawn",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_OTHER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated other cash activities related to the fund.",
                    "long_name": "Aggregated Fund Cash Other",
                    "unit": "fund currency"
                }
            },
            "FUND_VALUE_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjustments made to the value of the fund assets.",
                    "long_name": "Aggregated Fund Value Adjustment",
                    "unit": "fund currency"
                }
            },
            "FUND_REMOVE_DBL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital written off or removed from the fund.",
                    "long_name": "Aggregated Fund Remove Double Count",
                    "unit": "fund currency"
                }
            },
            "FUND_CONDITIONAL_ACQ_EQ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital set aside for potential future acquisitions.",
                    "long_name": "Aggregated Conditional Acquisitions (Equity)",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total drawn debt associated with the fund.",
                    "long_name": "Aggregated Fund Drawn RCF/Debt",
                    "unit": "fund currency"
                }
            },
            "UNDRAWN_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated undrawn authorized debt for the fund.",
                    "long_name": "Aggregated Undrawn Fund RCF/Debt",
                    "unit": "fund currency"
                }
            },
            "TOTAL_COMMITTED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total amount of capital committed by investors.",
                    "long_name": "Aggregated Fund Commitments",
                    "unit": "fund currency"
                }
            },
            "EXPIRED_COMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital commitments that have expired.",
                    "long_name": "Aggregated Expired Commitments",
                    "unit": "fund currency"
                }
            },
            "TOTAL_CALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total capital called from investors.",
                    "long_name": "Aggregated Total Called Subscriptions",
                    "unit": "fund currency"
                }
            },
            "RETURN_OF_CALL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated returns received from calls made to investors.",
                    "long_name": "Aggregated Returned Calls",
                    "unit": "fund currency"
                }
            },
            "TOTAL_RECALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total capital recalled from investors.",
                    "long_name": "Aggregated Total Recalled Capital",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_CAPITAL_HR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital of the fund held in reserves.",
                    "long_name": "Aggregated Fund Capital Held Reserves",
                    "unit": "fund currency"
                }
            },
            "EQ_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated valuation of the equity portion of the portfolio assets.",
                    "long_name": "Aggregated Asset Equity Value",
                    "unit": "n/a"
                }
            },
            "LN_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated valuation of the loan or debt portion of the portfolio assets.",
                    "long_name": "Aggregated Asset Shareholder Loan Value",
                    "unit": "n/a"
                }
            },
            "LOAN_BALANCE": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated outstanding loan balance associated with the assets.",
                    "long_name": "Aggregated Asset Loan Balance",
                    "unit": "n/a"
                }
            },
            "ASSET_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total debt directly associated with the assets.",
                    "long_name": "Aggregated Asset Level Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_DEBT_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated debt commitment for the assets.",
                    "long_name": "Aggregated Asset Committed Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_EQUITY_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated equity commitment for the assets.",
                    "long_name": "Aggregated Asset Committed Equity",
                    "unit": "n/a"
                }
            },
            "EXTERNAL_DEBT_COMMIT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated commitment to external debt associated with the assets.",
                    "long_name": "Aggregated External Asset Debt Commitment",
                    "unit": "n/a"
                }
            },
            "ADJ_UNDRAWNCOMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjusted undrawn capital commitment for the assets.",
                    "long_name": "Aggregated Asset Commitment Adjustment",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash held at the asset level.",
                    "long_name": "Aggregated Asset Level Cash",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjusted cash at the asset level.",
                    "long_name": "Aggregated Asset Level Cash Adjusted",
                    "unit": "n/a"
                }
            },
            "ASSET_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Net Asset Value of the aggregated assets (Equity + Loan Valuations + Loan Balance).",
                    "long_name": "Calculated Asset NAV",
                    "unit": "n/a"
                }
            },
            "FUND_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Net Asset Value of the fund, rolling up cash, adjustments, valuations, and deducting debt.",
                    "long_name": "Calculated Fund NAV",
                    "unit": "fund currency"
                }
            },
            "FUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Funds Under Management (Fund NAV + Capital Held in Reserves).",
                    "long_name": "Calculated Funds Under Management",
                    "unit": "fund currency"
                }
            },
            "PORTFOLIO_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Gross Asset Value of the portfolio (Asset NAV + Asset Debt).",
                    "long_name": "Calculated Portfolio GAV",
                    "unit": "n/a"
                }
            },
            "FUND_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Gross Asset Value of the fund (Portfolio GAV + Fund level cash and adjustments).",
                    "long_name": "Calculated Fund GAV",
                    "unit": "fund currency"
                }
            },
            "AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Assets Under Management (Fund GAV + Committed Capital + Adjustments).",
                    "long_name": "Calculated Assets Under Management",
                    "unit": "n/a"
                }
            },
            "DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Dry Powder representing available, unallocated capital.",
                    "long_name": "Calculated Dry Powder",
                    "unit": "n/a"
                }
            },
            "CHECK_ASSET_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Asset NAV. Target value is 0.",
                    "long_name": "Asset NAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUND_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Fund NAV. Target value is 0.",
                    "long_name": "Fund NAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Funds Under Management. Target value is 0.",
                    "long_name": "FUM Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_PORTFOLIO_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Portfolio GAV. Target value is 0.",
                    "long_name": "Portfolio GAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUND_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Fund GAV. Target value is 0.",
                    "long_name": "Fund GAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for AUM. Target value is 0.",
                    "long_name": "AUM Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Dry Powder. Target value is 0.",
                    "long_name": "Dry Powder Variance Check",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the latest bronze record in this aggregation was ingested into the Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "aum_metrics_enriched": {
        "table": {
            "comment": "Silver layer enriched AUM table. Joins Core Data Model (CDM) identifiers, handles eliminations, and aggregates detailed asset, technology, and location classifications."
        },
        "columns": {
            "asset_names": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of all original asset names within this portfolio grouping.",
                    "long_name": "All Asset Names",
                    "unit": "n/a"
                }
            },
            "reporting_asset_names": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of asset names utilized for reporting dashboards.",
                    "long_name": "Reporting Asset Names",
                    "unit": "n/a"
                }
            },
            "asset_types": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of original asset structural types.",
                    "long_name": "All Asset Types",
                    "unit": "n/a"
                }
            },
            "reporting_asset_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Grouped and simplified asset type (e.g., Platform) for reporting.",
                    "long_name": "Reporting Asset Type",
                    "unit": "n/a"
                }
            },
            "asset_lifecycle_phases": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of original operational lifecycle phases for the assets.",
                    "long_name": "All Lifecycle Phases",
                    "unit": "n/a"
                }
            },
            "reporting_asset_lifecycle_phase": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Prioritized and grouped lifecycle phase for reporting (e.g., Operational, In Construction).",
                    "long_name": "Reporting Lifecycle Phase",
                    "unit": "n/a"
                }
            },
            "asset_ownership_phases": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of original ownership phases for the assets.",
                    "long_name": "All Ownership Phases",
                    "unit": "n/a"
                }
            },
            "reporting_asset_ownership_phase": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Grouped ownership phase for reporting (e.g., Acquired).",
                    "long_name": "Reporting Ownership Phase",
                    "unit": "n/a"
                }
            },
            "asset_countries": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of original geographic locations for the assets.",
                    "long_name": "All Asset Countries",
                    "unit": "n/a"
                }
            },
            "reporting_asset_country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Grouped and standardized geographic location for reporting (e.g., UK, Multi-Country).",
                    "long_name": "Reporting Country",
                    "unit": "n/a"
                }
            },
            "asset_technologies": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Semi-colon separated list of original technologies utilized by the assets.",
                    "long_name": "All Asset Technologies",
                    "unit": "n/a"
                }
            },
            "reporting_asset_technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Grouped and standardized technology classification for reporting (e.g., Solar, Multi-Tech Renewables).",
                    "long_name": "Reporting Technology",
                    "unit": "n/a"
                }
            },
            "asset_details": {
                "schema": {"data_type": "ARRAY<STRUCT<name:STRING,type:STRING,country:STRING,technology:STRING,lifecycle_phase:STRING,ownership_phase:STRING>>"},
                "description": {
                    "comment": "Complex array of structs preserving granular metadata for every asset tied to this portfolio record.",
                    "long_name": "Asset Details Array",
                    "unit": "n/a"
                }
            },
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Mapped Core Data Model (CDM) Fund GUID.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "cdm_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Mapped Core Data Model (CDM) Portfolio GUID.",
                    "long_name": "CDM Portfolio ID",
                    "unit": "n/a"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Standardized Core Data Model (CDM) Fund display name.",
                    "long_name": "CDM Fund Name",
                    "unit": "n/a"
                }
            },
            "cdm_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Standardized Core Data Model (CDM) Portfolio display name.",
                    "long_name": "CDM Portfolio Name",
                    "unit": "n/a"
                }
            },
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "ef_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the portfolio.",
                    "long_name": "eFront Portfolio GUID",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund short name from eFront.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "ef_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the portfolio from eFront.",
                    "long_name": "eFront Portfolio Name",
                    "unit": "n/a"
                }
            },
            "REPORT_DATE": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "Date of the report for the aggregated metrics.",
                    "long_name": "Report Date",
                    "unit": "n/a"
                }
            },
            "FUND_CASH_UP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash in the fund allocated for investments (Upstream).",
                    "long_name": "Aggregated Fund Cash Upstream",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_DOWN": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash drawn down from the fund (Downstream).",
                    "long_name": "Aggregated Fund Cash Downstream",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_EXP": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash expenses related to the fund.",
                    "long_name": "Aggregated Fund Cash Expenses",
                    "unit": "fund currency"
                }
            },
            "FUND_CAPITAL_DR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital drawn from investors for investments.",
                    "long_name": "Aggregated Fund Capital Drawn",
                    "unit": "fund currency"
                }
            },
            "FUND_CASH_OTHER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated other cash activities related to the fund.",
                    "long_name": "Aggregated Fund Cash Other",
                    "unit": "fund currency"
                }
            },
            "FUND_VALUE_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjustments made to the value of the fund assets.",
                    "long_name": "Aggregated Fund Value Adjustment",
                    "unit": "fund currency"
                }
            },
            "FUND_REMOVE_DBL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital written off or removed from the fund.",
                    "long_name": "Aggregated Fund Remove Double Count",
                    "unit": "fund currency"
                }
            },
            "FUND_CONDITIONAL_ACQ_EQ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital set aside for potential future acquisitions.",
                    "long_name": "Aggregated Conditional Acquisitions (Equity)",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total drawn debt associated with the fund.",
                    "long_name": "Aggregated Fund Drawn RCF/Debt",
                    "unit": "fund currency"
                }
            },
            "UNDRAWN_FUND_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated undrawn authorized debt for the fund.",
                    "long_name": "Aggregated Undrawn Fund RCF/Debt",
                    "unit": "fund currency"
                }
            },
            "TOTAL_COMMITTED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total amount of capital committed by investors.",
                    "long_name": "Aggregated Fund Commitments",
                    "unit": "fund currency"
                }
            },
            "EXPIRED_COMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital commitments that have expired.",
                    "long_name": "Aggregated Expired Commitments",
                    "unit": "fund currency"
                }
            },
            "TOTAL_CALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total capital called from investors.",
                    "long_name": "Aggregated Total Called Subscriptions",
                    "unit": "fund currency"
                }
            },
            "RETURN_OF_CALL": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated returns received from calls made to investors.",
                    "long_name": "Aggregated Returned Calls",
                    "unit": "fund currency"
                }
            },
            "TOTAL_RECALLED": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total capital recalled from investors.",
                    "long_name": "Aggregated Total Recalled Capital",
                    "unit": "fund currency"
                }
            },
            "TOTAL_FUND_CAPITAL_HR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated capital of the fund held in reserves.",
                    "long_name": "Aggregated Fund Capital Held Reserves",
                    "unit": "fund currency"
                }
            },
            "EQ_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated valuation of the equity portion of the portfolio assets.",
                    "long_name": "Aggregated Asset Equity Value",
                    "unit": "n/a"
                }
            },
            "LN_VALUATION": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated valuation of the loan or debt portion of the portfolio assets.",
                    "long_name": "Aggregated Asset Shareholder Loan Value",
                    "unit": "n/a"
                }
            },
            "LOAN_BALANCE": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated outstanding loan balance associated with the assets.",
                    "long_name": "Aggregated Asset Loan Balance",
                    "unit": "n/a"
                }
            },
            "ASSET_DEBT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated total debt directly associated with the assets.",
                    "long_name": "Aggregated Asset Level Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_DEBT_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated debt commitment for the assets.",
                    "long_name": "Aggregated Asset Committed Debt",
                    "unit": "n/a"
                }
            },
            "ASSET_EQUITY_CMT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated equity commitment for the assets.",
                    "long_name": "Aggregated Asset Committed Equity",
                    "unit": "n/a"
                }
            },
            "EXTERNAL_DEBT_COMMIT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated commitment to external debt associated with the assets.",
                    "long_name": "Aggregated External Asset Debt Commitment",
                    "unit": "n/a"
                }
            },
            "ADJ_UNDRAWNCOMMITMENT": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjusted undrawn capital commitment for the assets.",
                    "long_name": "Aggregated Asset Commitment Adjustment",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated cash held at the asset level.",
                    "long_name": "Aggregated Asset Level Cash",
                    "unit": "n/a"
                }
            },
            "ASSET_LEVEL_CASH_ADJ": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Aggregated adjusted cash at the asset level.",
                    "long_name": "Aggregated Asset Level Cash Adjusted",
                    "unit": "n/a"
                }
            },
            "ASSET_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Net Asset Value of the aggregated assets (Equity + Loan Valuations + Loan Balance).",
                    "long_name": "Calculated Asset NAV",
                    "unit": "n/a"
                }
            },
            "FUND_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Net Asset Value of the fund, rolling up cash, adjustments, valuations, and deducting debt.",
                    "long_name": "Calculated Fund NAV",
                    "unit": "fund currency"
                }
            },
            "FUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Funds Under Management (Fund NAV + Capital Held in Reserves).",
                    "long_name": "Calculated Funds Under Management",
                    "unit": "fund currency"
                }
            },
            "PORTFOLIO_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Gross Asset Value of the portfolio (Asset NAV + Asset Debt).",
                    "long_name": "Calculated Portfolio GAV",
                    "unit": "n/a"
                }
            },
            "FUND_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Gross Asset Value of the fund (Portfolio GAV + Fund level cash and adjustments).",
                    "long_name": "Calculated Fund GAV",
                    "unit": "fund currency"
                }
            },
            "AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Assets Under Management (Fund GAV + Committed Capital + Adjustments).",
                    "long_name": "Calculated Assets Under Management",
                    "unit": "n/a"
                }
            },
            "DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Calculated Dry Powder representing available, unallocated capital.",
                    "long_name": "Calculated Dry Powder",
                    "unit": "n/a"
                }
            },
            "CHECK_ASSET_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Asset NAV. Target value is 0.",
                    "long_name": "Asset NAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUND_NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Fund NAV. Target value is 0.",
                    "long_name": "Fund NAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Funds Under Management. Target value is 0.",
                    "long_name": "FUM Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_PORTFOLIO_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Portfolio GAV. Target value is 0.",
                    "long_name": "Portfolio GAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_FUND_GAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Fund GAV. Target value is 0.",
                    "long_name": "Fund GAV Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_AUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for AUM. Target value is 0.",
                    "long_name": "AUM Variance Check",
                    "unit": "n/a"
                }
            },
            "CHECK_DRY_POWDER": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Data quality variance check for Dry Powder. Target value is 0.",
                    "long_name": "Dry Powder Variance Check",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the latest bronze record in this aggregation was ingested into the Data Lake.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            },
            "inv_pipeline_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Pipeline classification mapped from the CDM Investment Portfolio.",
                    "long_name": "Investment Pipeline Type",
                    "unit": "n/a"
                }
            },
            "invp_investment_strategy": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investment strategy mapped from the CDM Investment Portfolio.",
                    "long_name": "Investment Strategy",
                    "unit": "n/a"
                }
            },
            "fund_structure_type_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Fund structure classification mapped from the CDM Fund Dimension.",
                    "long_name": "Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "backfilled": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Boolean flag indicating if any asset or dimensional metadata was forward-filled from a previous period due to missing source data.",
                    "long_name": "Metadata Backfilled Flag",
                    "unit": "n/a"
                }
            }
        }
    },

    "fx_vs_gbp": {
        "table": {
            "comment": "Silver layer trusted FX reference dataset providing standardized exchange rates from various source currencies into British Pounds (GBP) by date."
        },
        "columns": {
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The standardized 3-letter currency code (e.g., USD, EUR, AUD) being converted into GBP.",
                    "long_name": "Standardized Currency Code",
                    "unit": "n/a"
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
            "Rate_to_Gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The normalized exchange rate multiplier representing the value of 1 unit of the source Currency in British Pounds (GBP).",
                    "long_name": "Exchange Rate to GBP",
                    "unit": "GBP"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },
    
    "track_record_fund": {
        "table": {
            "comment": "Silver layer fund track record table. Combines eFront track record metrics and datapoints, mapping them to the Core Data Model (CDM) and pivoting historical December 31st values by year."
        },
        "columns": {
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fund sourced directly from eFront.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The short name or ticker used internally to identify the fund in eFront.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The mapped universal identifier for the fund according to the Core Data Model.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The standardized display name for the fund according to the Core Data Model.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "fund_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The base reporting currency of the fund (e.g., GBP, USD).",
                    "long_name": "Fund Base Currency",
                    "unit": "n/a"
                }
            },
            "inception_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The official start or launch date of the fund.",
                    "long_name": "Fund Inception Date",
                    "unit": "n/a"
                }
            },
            "fund_is_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Flag indicating if the fund is currently active based on its start date, end date, and the reporting date.",
                    "long_name": "Fund Is Active Flag",
                    "unit": "n/a"
                }
            },
            "raw_metric": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The normalized, raw system name of the performance metric before display mapping.",
                    "long_name": "Raw Metric System Name",
                    "unit": "n/a"
                }
            },
            "metric": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The clean, human-readable display name for the performance metric (e.g., Total NAV Return).",
                    "long_name": "Metric Display Name",
                    "unit": "n/a"
                }
            },
            "latest_metric_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The reporting date associated with the most recent non-null value for this specific metric.",
                    "long_name": "Latest Metric Date",
                    "unit": "n/a"
                }
            },
            "latest_metric_value": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The most recent non-null recorded value for this specific metric.",
                    "long_name": "Latest Metric Value",
                    "unit": "Varies by metric"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "track_record_metric_dictionary": {
        "table": {
            "comment": "Silver layer dictionary mapping raw performance and operational metrics from eFront datasets to their normalized forms and standardized Core Data Model display names."
        },
        "columns": {
            "domain": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The entity domain the metric applies to (e.g., fund).",
                    "long_name": "Entity Domain",
                    "unit": "n/a"
                }
            },
            "source_system_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Core Data Model (CDM) identifier for the source system generating the metric (e.g., SRCE_SYST_1001 for eFront).",
                    "long_name": "Source System ID",
                    "unit": "n/a"
                }
            },
            "dataset_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific source dataset or table where the raw metric originated (e.g., datapointsexport, track_record_fund).",
                    "long_name": "Source Dataset Name",
                    "unit": "n/a"
                }
            },
            "normalised_metric": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The raw metric name, normalized to lowercase with spaces and special characters replaced by underscores.",
                    "long_name": "Normalized Metric Name",
                    "unit": "n/a"
                }
            },
            "metric": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The standardized, human-readable display name mapped via the Core Data Model reference table.",
                    "long_name": "Standardized Metric Display Name",
                    "unit": "n/a"
                }
            },
            "definition": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The definition of the metric.",
                    "long_name": "Metric Definition",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this Silver dictionary table was computed and refreshed.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    }
}
