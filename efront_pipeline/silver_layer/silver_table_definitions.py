silver_table_definitions = {
    "aum_capital_injections_and_disposals_transactions": {
        "table": {
            "comment": "Silver layer table of individual capital injection and disposal transactions (subscriptions, advances, redemptions), mapped to canonical CDM Fund/Portfolio identifiers, converted into GBP, and bucketed into the AUM reporting period they fall into. GBP conversion uses the fx_vs_gbp rate as of the month-end of the transaction date, not a spot/daily rate — see fx_ref_date and used_rate_to_gbp."
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
                    "comment": "The mapped universal identifier for the portfolio according to the Core Data Model.",
                    "long_name": "CDM Portfolio ID",
                    "unit": "n/a"
                }
            },
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund, after applying manual corrections for known naming exceptions (e.g. ORIP, OEOW).",
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
                    "comment": "The fund short name from eFront, after applying manual corrections for known naming exceptions (e.g. ORIP -> Sky, OEOW -> Vector).",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "ef_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific project or portfolio name the transaction relates to, as sourced from eFront.",
                    "long_name": "eFront Portfolio Name",
                    "unit": "n/a"
                }
            },
            "investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investor entity, fund, or SPV executing the transaction.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction type (e.g. EQ - Purchase/Subscription, LN - Advance, EQ - Sale/Redemption).",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "amount": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The transaction amount in the fund's base currency, prior to GBP conversion.",
                    "long_name": "Transaction Amount (Base Currency)",
                    "unit": "fund currency"
                }
            },
            "transaction_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The actual effective/execution date of the transaction, as sourced from eFront.",
                    "long_name": "Transaction Effective Date",
                    "unit": "n/a"
                }
            },
            "transaction_month_end": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The month-end date of transaction_date (e.g. any date in January maps to 31 Jan). Used both to select the FX rate applied and to bucket the transaction into an AUM reporting period.",
                    "long_name": "Transaction Month-End Date",
                    "unit": "n/a"
                }
            },
            "currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's base currency for this transaction, sourced from the fund dimension table.",
                    "long_name": "Fund Base Currency",
                    "unit": "n/a"
                }
            },
            "fx_ref_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The Ref_Date of the fx_vs_gbp row matched to this transaction, equal to the month-end of transaction_date. NOT the transaction's own date - the FX rate is pinned to month-end, not a daily/spot rate.",
                    "long_name": "FX Rate Reference Date",
                    "unit": "n/a"
                }
            },
            "used_rate_to_gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The exchange rate applied to convert amount into GBP. Hardcoded to 1.0 when currency is GBP; otherwise the fx_vs_gbp Rate_to_Gbp for (currency, month-end of transaction_date). NULL if no matching fx_vs_gbp row exists for a non-GBP currency (there is no fallback to 1.0 in that case).",
                    "long_name": "Applied FX Rate to GBP",
                    "unit": "rate"
                }
            },
            "amount_gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The transaction amount converted into GBP using used_rate_to_gbp. NULL if used_rate_to_gbp could not be resolved.",
                    "long_name": "Transaction Amount (GBP)",
                    "unit": "GBP"
                }
            },
            "tx_effective_report_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The AUM reporting period (Report_Date from aum_metrics) that this transaction has been bucketed into, based on where transaction_month_end falls relative to the fund/portfolio's AUM report date periods.",
                    "long_name": "Effective AUM Report Date",
                    "unit": "n/a"
                }
            },
            "tx_bucket_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the transaction was successfully bucketed into an AUM reporting period ('bucketed_tx') or could not be matched to one ('unbucketed_tx').",
                    "long_name": "Transaction Bucket Status",
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
            "comment": "Silver layer trusted FX reference dataset providing standardized exchange rates from various source currencies into British Pounds (GBP) by date. Normalizes bronze FX pairs (which may be quoted GBP->Currency or Currency->GBP) into a single consistent Currency->GBP rate per (Currency, Ref_Date), for use by downstream models (e.g. aum_capital_injections_and_disposals_transactions) that need to convert local-currency amounts into GBP as of a given reference date."
        },
        "columns": {
            "currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The standardized 3-letter currency code (e.g., USD, EUR, AUD) being converted into GBP. Derived from bronze fx's Source_Currency/Destination_Curr name via curr_map, taking whichever side of the pair is the non-GBP currency.",
                    "long_name": "Standardized Currency Code",
                    "unit": "n/a"
                }
            },
            "ref_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The reference date and time that the exchange rate applies to, passed through from bronze fx.Ref_Date.",
                    "long_name": "Reference Date",
                    "unit": "n/a"
                }
            },
            "rate_to_gbp": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The normalized exchange rate multiplier representing the value of 1 unit of the source currency in British Pounds (GBP). For pairs already quoted Currency->GBP, this is bronze fx.Fx_Rate as-is; for pairs quoted GBP->Currency, it is 1/Fx_Rate. When both directions exist for the same (currency, ref_date), the direct quote is kept via QUALIFY ROW_NUMBER() OVER (PARTITION BY currency, ref_date ORDER BY is_direct DESC) = 1.",
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
                    "comment": "The clean, human-readable display name for the performance metric (e.g., Total NAV Return). Derived via a LEFT JOIN from metric_display_names on raw_metric; NULL if no display-name mapping exists for that raw metric.",
                    "long_name": "Metric Display Name",
                    "unit": "n/a"
                }
            },
            "definition": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The business definition of the metric, looked up via a LEFT JOIN from metric_dictionary (md.definition) on raw_metric = md.normalised_metric; NULL if the raw metric has no dictionary entry.",
                    "long_name": "Metric Definition",
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
            "2000": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2000 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2000",
                    "unit": "Varies by metric"
                }
            },
            "2001": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2001 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2001",
                    "unit": "Varies by metric"
                }
            },
            "2002": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2002 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2002",
                    "unit": "Varies by metric"
                }
            },
            "2003": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2003 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2003",
                    "unit": "Varies by metric"
                }
            },
            "2004": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2004 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2004",
                    "unit": "Varies by metric"
                }
            },
            "2005": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2005 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2005",
                    "unit": "Varies by metric"
                }
            },
            "2006": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2006 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2006",
                    "unit": "Varies by metric"
                }
            },
            "2007": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2007 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2007",
                    "unit": "Varies by metric"
                }
            },
            "2008": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2008 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2008",
                    "unit": "Varies by metric"
                }
            },
            "2009": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2009 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2009",
                    "unit": "Varies by metric"
                }
            },
            "2010": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2010 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2010",
                    "unit": "Varies by metric"
                }
            },
            "2011": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2011 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2011",
                    "unit": "Varies by metric"
                }
            },
            "2012": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2012 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2012",
                    "unit": "Varies by metric"
                }
            },
            "2013": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2013 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2013",
                    "unit": "Varies by metric"
                }
            },
            "2014": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2014 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2014",
                    "unit": "Varies by metric"
                }
            },
            "2015": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2015 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2015",
                    "unit": "Varies by metric"
                }
            },
            "2016": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2016 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2016",
                    "unit": "Varies by metric"
                }
            },
            "2017": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2017 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2017",
                    "unit": "Varies by metric"
                }
            },
            "2018": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2018 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2018",
                    "unit": "Varies by metric"
                }
            },
            "2019": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2019 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2019",
                    "unit": "Varies by metric"
                }
            },
            "2020": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2020 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2020",
                    "unit": "Varies by metric"
                }
            },
            "2021": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2021 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2021",
                    "unit": "Varies by metric"
                }
            },
            "2022": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2022 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2022",
                    "unit": "Varies by metric"
                }
            },
            "2023": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2023 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2023",
                    "unit": "Varies by metric"
                }
            },
            "2024": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2024 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2024",
                    "unit": "Varies by metric"
                }
            },
            "2025": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2025 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2025",
                    "unit": "Varies by metric"
                }
            },
            "2026": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2026 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2026",
                    "unit": "Varies by metric"
                }
            },
            "2027": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2027 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2027",
                    "unit": "Varies by metric"
                }
            },
            "2028": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2028 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2028",
                    "unit": "Varies by metric"
                }
            },
            "2029": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2029 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2029",
                    "unit": "Varies by metric"
                }
            },
            "2030": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2030 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2030",
                    "unit": "Varies by metric"
                }
            },
            "2031": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2031 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2031",
                    "unit": "Varies by metric"
                }
            },
            "2032": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2032 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2032",
                    "unit": "Varies by metric"
                }
            },
            "2033": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2033 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2033",
                    "unit": "Varies by metric"
                }
            },
            "2034": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2034 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2034",
                    "unit": "Varies by metric"
                }
            },
            "2035": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "This metric's value as of 31 December 2035 for the fund. Produced by pivoting the melted per-metric time series (PIVOT ... FOR report_date_str IN ('2000', ..., '2035')) filtered to rows where month(report_date)=12 AND day(report_date)=31; NULL if no 31-Dec snapshot exists for this fund/metric/year.",
                    "long_name": "Metric Value at 31 Dec 2035",
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
    },
    "transactions": {
        "table": {
            "comment": "Data table containing granular, itemized financial transaction records (e.g., loans, advances, repayments)."
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
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investee company.",
                    "long_name": "Investee Company GUID",
                    "unit": "n/a"
                }
            },
            "company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investee company or entity involved in the transaction.",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "company_investor_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investor entity.",
                    "long_name": "Investor GUID",
                    "unit": "n/a"
                }
            },
            "company_investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investor entity, fund, or SPV executing the transaction.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "investment_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific project or portfolio name the transaction relates to (e.g., Gaishecke).",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The exact action or accounting type of the transaction (e.g., LN - Repayment, LN - Advance).",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "oegen_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of OEGEN (Operational Entity Generation) associated with the transaction",
                    "long_name": "OEGEN Type",
                    "unit": "n/a"
                }
            },
            "description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A granular description of the transaction's purpose (e.g., Credit Facility Loan).",
                    "long_name": "Transaction Description",
                    "unit": "n/a"
                }
            },
            "complete_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The overarching classification or name of the transaction (e.g., Shareholder Loan).",
                    "long_name": "Transaction Name",
                    "unit": "n/a"
                }
            },
            "instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific financial instrument utilized (e.g., Shareholder Loan, Equity).",
                    "long_name": "Transaction Instrument",
                    "unit": "n/a"
                }
            },
            "investment_details": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The grouping or portfolio segment associated with the transaction (e.g., ORIP).",
                    "long_name": "Investment Grouping Details",
                    "unit": "n/a"
                }
            },
            "index": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The sequential index or ordering number of the transaction.",
                    "long_name": "Transaction Index",
                    "unit": "n/a"
                }
            },
            "draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the transaction is in a draft or uncommitted state (e.g., True/False).",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            },
            "instrument_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The local currency in which the instrument is denominated (e.g., EUR, GBP).",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "exchange_rate_instr_investor": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The specific FX exchange rate applied to convert the instrument currency to the investor currency.",
                    "long_name": "Applied Exchange Rate",
                    "unit": "rate"
                }
            },
            "amount": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total financial amount of the transaction in the base or reporting currency.",
                    "long_name": "Transaction Amount (Base)",
                    "unit": "base currency"
                }
            },
            "amount_instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The financial amount of the transaction expressed in the instrument's local currency.",
                    "long_name": "Instrument Amount (Local)",
                    "unit": "instrument currency"
                }
            },
            "valuation": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The assessed valuation of the transaction/instrument in the base currency.",
                    "long_name": "Transaction Valuation (Base)",
                    "unit": "base currency"
                }
            },
            "valuation_instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The assessed valuation of the transaction/instrument in the local instrument currency.",
                    "long_name": "Instrument Valuation (Local)",
                    "unit": "instrument currency"
                }
            },
            "commitment": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total capital commitment associated with this transaction or instrument line.",
                    "long_name": "Transaction Commitment (Base)",
                    "unit": "base currency"
                }
            },
            "commitment_instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total capital commitment expressed in the instrument's local currency.",
                    "long_name": "Instrument Commitment (Local)",
                    "unit": "instrument currency"
                }
            },
            "exclude_transaction_costs": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating whether transaction costs are excluded from the calculated amounts.",
                    "long_name": "Exclude Transaction Costs Flag",
                    "unit": "n/a"
                }
            },
            "aum_reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the transaction is included in Assets Under Management reporting (e.g., True/False).",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The actual effective or execution date of the transaction.",
                    "long_name": "Transaction Effective Date",
                    "unit": "n/a"
                }
            },
            "created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the transaction record was created in the source system.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the transaction record.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the transaction in the source system.",
                    "long_name": "Source Record Modification Timestamp",
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
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "transactions_paid": {
        "table": {
            "comment": "Data table containing itemized records of paid transactions, tracking payment amounts, dates, and associated entities or instruments."
        },
        "columns": {
            "aum_reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the transaction is included in Assets Under Management reporting (e.g., True/False)",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "amount_paid": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The actual financial amount that was paid",
                    "long_name": "Amount Paid",
                    "unit": "currency"
                }
            },
            "amount_transaction": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total financial amount of the transaction",
                    "long_name": "Transaction Amount",
                    "unit": "instrument currency"
                }
            },
            "company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investee company or entity involved in the transaction",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investee company",
                    "long_name": "Investee Company GUID",
                    "unit": "n/a"
                }
            },
            "company_investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investor entity, fund, or SPV associated with the payment",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "company_investor_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investor entity",
                    "long_name": "Investor GUID",
                    "unit": "n/a"
                }
            },
            "complete_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The overarching classification or complete name of the transaction (e.g., Shareholder Loan)",
                    "long_name": "Transaction Name",
                    "unit": "n/a"
                }
            },
            "created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the paid transaction record was created in the source system",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "date_paid": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact date when the transaction amount was actually paid",
                    "long_name": "Payment Date",
                    "unit": "n/a"
                }
            },
            "description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A granular description of the paid transaction's purpose (e.g., Credit Facility Loan)",
                    "long_name": "Transaction Description",
                    "unit": "n/a"
                }
            },
            "effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The effective or execution date of the transaction",
                    "long_name": "Transaction Effective Date",
                    "unit": "n/a"
                }
            },
            "index": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The sequential index or ordering number of the transaction",
                    "long_name": "Transaction Index",
                    "unit": "n/a"
                }
            },
            "instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific financial instrument utilized",
                    "long_name": "Transaction Instrument",
                    "unit": "n/a"
                }
            },
            "instrument_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The local currency in which the instrument is denominated (e.g., EUR, GBP)",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "investment_details": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The grouping or portfolio segment associated with the transaction",
                    "long_name": "Investment Grouping Details",
                    "unit": "n/a"
                }
            },
            "modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the paid transaction record",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the transaction in the source system",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "investment_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific project or portfolio name the transaction relates to",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "transaction_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the individual transaction",
                    "long_name": "Transaction GUID",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The exact action or accounting type of the transaction (e.g., LN - Cash Interest)",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the transaction is in a draft or uncommitted state",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            },
            "exclude_transaction_costs": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating whether transaction costs are excluded from the amounts",
                    "long_name": "Exclude Transaction Costs Flag",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "company": {
        "table": {
            "comment": "Silver layer master data table for investee companies and SPVs. SQL is SELECT * FROM bronze company LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_company ON Company_IQId = source_company_id WHERE source_system_id = 'SRCE_SYST_1001' — every bronze eFront company column is passed through unchanged (documented below), and cdm_company_id is added from the CDM mapping table. Note: bronze_mapping_company is an external CDM table and the wildcard join could in principle surface additional CDM-side columns beyond cdm_company_id if that table's schema changes; only the columns actually observed in the current pipeline output are documented here."
        },
        "columns": {
            "cdm_company_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The company's canonical identifier in the company-wide Core Data Model. Sourced from oegen_data_prod_prod.core_data_model.bronze_mapping_company (column cdm_company_id) via a LEFT JOIN on Company_IQId = source_company_id, filtered to source_system_id = 'SRCE_SYST_1001'; NULL if eFront's company has not yet been mapped into the CDM.",
                    "long_name": "CDM Company ID",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the company. Passed through unchanged from bronze and used as the join key into the CDM company mapping table.",
                    "long_name": "eFront Company GUID",
                    "unit": "n/a"
                }
            },
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full registered name of the company or SPV, passed through unchanged from bronze.",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "AUM_Reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the company is included in Assets Under Management reporting (True/False). Passed through unchanged from bronze eFront company.AUM_Reporting (via SELECT *).",
                    "long_name": "AUM Reporting",
                    "unit": "n/a"
                }
            },
            "Accounts_Due": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month or reporting period when the company's accounts are due (e.g., March, September). Passed through unchanged from bronze eFront company.Accounts_Due (via SELECT *).",
                    "long_name": "Accounts Due",
                    "unit": "n/a"
                }
            },
            "Address": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Primary registered address of the company. Passed through unchanged from bronze eFront company.Address (via SELECT *).",
                    "long_name": "Address",
                    "unit": "n/a"
                }
            },
            "Address2": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Secondary registered address or suite details. Passed through unchanged from bronze eFront company.Address2 (via SELECT *).",
                    "long_name": "Address2",
                    "unit": "n/a"
                }
            },
            "Asset_Class": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The asset class to which the company belongs (e.g., Developer, Asset). Passed through unchanged from bronze eFront company.Asset_Class (via SELECT *).",
                    "long_name": "Asset Class",
                    "unit": "n/a"
                }
            },
            "City": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The city where the company is registered or headquartered. Passed through unchanged from bronze eFront company.City (via SELECT *).",
                    "long_name": "City",
                    "unit": "n/a"
                }
            },
            "Company_ID": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal human-readable identifier for the company (e.g., COMP_1668). Passed through unchanged from bronze eFront company.Company_ID (via SELECT *).",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "Company_Number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Official government or state registration number of the company. Passed through unchanged from bronze eFront company.Company_Number (via SELECT *).",
                    "long_name": "Company Number",
                    "unit": "n/a"
                }
            },
            "Company_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Structural classification of the company (e.g., Portfolio Holding Company, SPV). Passed through unchanged from bronze eFront company.Company_Type (via SELECT *).",
                    "long_name": "Company Type",
                    "unit": "n/a"
                }
            },
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country where the company is currently registered. Passed through unchanged from bronze eFront company.Country (via SELECT *).",
                    "long_name": "Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the company record was originally created in the source system. Passed through unchanged from bronze eFront company.Created_on (via SELECT *).",
                    "long_name": "Created on",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The base currency used by the company for financial reporting (e.g., GBP). Passed through unchanged from bronze eFront company.Currency (via SELECT *).",
                    "long_name": "Currency",
                    "unit": "n/a"
                }
            },
            "Financial_year_end": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month marking the end of the company's financial year (e.g., June, December). Passed through unchanged from bronze eFront company.Financial_year_end (via SELECT *).",
                    "long_name": "Financial year end",
                    "unit": "n/a"
                }
            },
            "House_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "House or building number associated with the company's registered address. Passed through unchanged from bronze eFront company.House_number (via SELECT *).",
                    "long_name": "House number",
                    "unit": "n/a"
                }
            },
            "Known_as": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative alias, abbreviation, or common trading name for the company. Passed through unchanged from bronze eFront company.Known_as (via SELECT *).",
                    "long_name": "Known as",
                    "unit": "n/a"
                }
            },
            "LEI": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The global Legal Entity Identifier (LEI) code for the company. Passed through unchanged from bronze eFront company.LEI (via SELECT *).",
                    "long_name": "LEI",
                    "unit": "n/a"
                }
            },
            "Legal_Form": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The designated legal structure of the entity (e.g., GBR - Ltd - Limited). Passed through unchanged from bronze eFront company.Legal_Form (via SELECT *).",
                    "long_name": "Legal Form",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the company record in eFront. Passed through unchanged from bronze eFront company.Modified_by (via SELECT *).",
                    "long_name": "Modified by",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last time the company record was updated in the source system. Passed through unchanged from bronze eFront company.Modified_on (via SELECT *).",
                    "long_name": "Modified on",
                    "unit": "n/a"
                }
            },
            "Number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative or legacy reference number for the company (often mirrors Company_ID). Passed through unchanged from bronze eFront company.Number (via SELECT *).",
                    "long_name": "Number",
                    "unit": "n/a"
                }
            },
            "Original_country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The original country of registration if the company has relocated its domicile. Passed through unchanged from bronze eFront company.Original_country (via SELECT *).",
                    "long_name": "Original country",
                    "unit": "n/a"
                }
            },
            "Other_names": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Any other historical or associated names used by the company. Passed through unchanged from bronze eFront company.Other_names (via SELECT *).",
                    "long_name": "Other names",
                    "unit": "n/a"
                }
            },
            "Site_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The internal system site or division the company belongs to (e.g., Business). Passed through unchanged from bronze eFront company.Site_Name (via SELECT *).",
                    "long_name": "Site Name",
                    "unit": "n/a"
                }
            },
            "Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The current operational or management status of the company (e.g., SPV: Managed Subsidiary). Passed through unchanged from bronze eFront company.Status (via SELECT *).",
                    "long_name": "Status",
                    "unit": "n/a"
                }
            },
            "Tax_Due": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The month or period when the company's tax filings are due. Passed through unchanged from bronze eFront company.Tax_Due (via SELECT *).",
                    "long_name": "Tax Due",
                    "unit": "n/a"
                }
            },
            "Technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary technology or operational sector of the company (e.g., Rooftop solar). Passed through unchanged from bronze eFront company.Technology (via SELECT *).",
                    "long_name": "Technology",
                    "unit": "n/a"
                }
            },
            "VAT_Registration_No": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The company's official Value Added Tax (VAT) registration number. Passed through unchanged from bronze eFront company.VAT_Registration_No (via SELECT *).",
                    "long_name": "VAT Registration No",
                    "unit": "n/a"
                }
            },
            "ZIP_Code": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The postal or ZIP code for the company's registered address. Passed through unchanged from bronze eFront company.ZIP_Code (via SELECT *).",
                    "long_name": "ZIP Code",
                    "unit": "n/a"
                }
            },
            "Lockbox_Date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The date associated with the company's lockbox or financial escrow mechanisms. Passed through unchanged from bronze eFront company.Lockbox_Date (via SELECT *).",
                    "long_name": "Lockbox Date",
                    "unit": "n/a"
                }
            },
            "TIN": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The Tax Identification Number for the company. Passed through unchanged from bronze eFront company.TIN (via SELECT *).",
                    "long_name": "TIN",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this bronze record was ingested into the Databricks Data Lake. Passed through unchanged from bronze eFront company.datalake_ingestion_timestamp (via SELECT *).",
                    "long_name": "datalake ingestion timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fund": {
        "table": {
            "comment": "Silver layer master data table for investment funds. Renames and reshapes the bronze eFront fund attributes into a consistent ef_*/cdm_* naming convention and enriches each fund with its canonical Core Data Model (CDM) fund identifier and display name, restricted to funds mapped against eFront's CDM source system ('SRCE_SYST_1001') with an active (END_AT IS NULL) CDM fund-core and fund-name record."
        },
        "columns": {
            "ef_fund_long": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full, official registered name of the fund. Renamed directly from bronze eFront fund.Fund.",
                    "long_name": "eFront Fund Full Name",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The abbreviated eFront short name/ticker used internally to identify the fund. Renamed directly from bronze eFront fund.Short_Name.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "cdm_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical display name in the Core Data Model. Sourced from oegen_data_prod_prod.core_data_model.bronze_fund_dim_names.fund_display_name via LEFT JOIN fund.Fund_IQId -> bronze_mapping_fund.source_fund_id -> bronze_fund_dim_core.fund_core_id -> bronze_fund_dim_names.primary_fund_name_id.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "fund_number": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "An alternative or legacy reference number for the fund. Renamed directly from bronze eFront fund.Number.",
                    "long_name": "Alternative Fund Number",
                    "unit": "n/a"
                }
            },
            "vintage_year": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The vintage year the fund was launched or began making investments, used for benchmarking. Renamed directly from bronze eFront fund.Vintage_Year.",
                    "long_name": "Fund Vintage Year",
                    "unit": "year"
                }
            },
            "fund_description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A textual description of the fund's investment strategy, mandate, or focus. Renamed directly from bronze eFront fund.Description.",
                    "long_name": "Fund Description",
                    "unit": "n/a"
                }
            },
            "oegen_fund_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Internal OEGEN classification of the fund's lifecycle or structural status (e.g., Close-Ended, Fully invested). Renamed directly from bronze eFront fund.OEGEN_Fund_Type.",
                    "long_name": "OEGEN Fund Classification",
                    "unit": "n/a"
                }
            },
            "legal_form": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legally designated structure of the fund (e.g., GBR - PLC - Public Limited Company). Renamed directly from bronze eFront fund.Legal_Form.",
                    "long_name": "Fund Legal Form",
                    "unit": "n/a"
                }
            },
            "AIF": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether the fund is classified as an Alternative Investment Fund (AIF) under AIFMD (e.g., Yes/No). Passed through unchanged from bronze eFront fund.AIF.",
                    "long_name": "Alternative Investment Fund Flag",
                    "unit": "n/a"
                }
            },
            "country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country where the fund is registered or domiciled. Renamed directly from bronze eFront fund.Country.",
                    "long_name": "Fund Domicile Country",
                    "unit": "n/a"
                }
            },
            "closing_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The date when the fund was officially closed to new investors or finalized its initial fundraising. Renamed directly from bronze eFront fund.Closing_Date.",
                    "long_name": "Fund Closing Date",
                    "unit": "n/a"
                }
            },
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund. Renamed directly from bronze eFront fund.Fund_IQId; used as the join key into the CDM fund mapping table and as the match key for this silver table.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "cdm_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical identifier in the Core Data Model. Sourced from oegen_data_prod_prod.core_data_model.bronze_mapping_fund.cdm_fund_id via the CDM mapping join described in the table comment.",
                    "long_name": "CDM Fund ID",
                    "unit": "n/a"
                }
            },
            "created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the fund record was originally created in the source system. Renamed directly from bronze eFront fund.Created_on.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the fund record in eFront. Renamed directly from bronze eFront fund.Modified_by.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "modified_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last time the fund record was updated in the source system. Renamed directly from bronze eFront fund.Modified_on.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when the underlying bronze record was ingested into the Databricks Data Lake, passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "investoraccount": {
        "table": {
            "comment": "Silver layer table of investor accounts held within each fund, renaming bronze eFront investoraccount columns to a lowercase silver naming convention. This is a straight rename/passthrough with no joins or aggregation."
        },
        "columns": {
            "ef_fund_long": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full, official registered name of the fund the account belongs to. Renamed directly from bronze eFront investoraccount.Fund.",
                    "long_name": "eFront Fund Full Name",
                    "unit": "n/a"
                }
            },
            "ef_fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The abbreviated eFront short name for the fund the account belongs to. Renamed directly from bronze eFront investoraccount.Short_Name.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investor. Renamed directly from bronze eFront investoraccount.Investor.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "investor_account": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific account held by the investor within the fund. Renamed directly from bronze eFront investoraccount.Investor_Account; used to join fundoperations records to this investor's classification data.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The currency in which the investor account is denominated. Renamed directly from bronze eFront investoraccount.Currency.",
                    "long_name": "Account Currency",
                    "unit": "n/a"
                }
            },
            "legal_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full legal name of the investor. Renamed directly from bronze eFront investoraccount.Legal_Name.",
                    "long_name": "Investor Legal Name",
                    "unit": "n/a"
                }
            },
            "investor_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The classification of the investor (e.g., individual, institutional, Feeder Fund). Renamed directly from bronze eFront investoraccount.Investor_Type; downstream fundoperations_daily falls back to 'Feeder Fund' when this is blank and the account name matches a known fund.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's country of residence/registration. Renamed directly from bronze eFront investoraccount.Country.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "tax_domicile": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The country where the investor is domiciled for tax purposes. Renamed directly from bronze eFront investoraccount.Tax_Domicile__Country_.",
                    "long_name": "Investor Tax Domicile Country",
                    "unit": "n/a"
                }
            },
            "aml_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's Anti-Money Laundering compliance status. Renamed directly from bronze eFront investoraccount.AML_Compliance_Status.",
                    "long_name": "AML Compliance Status",
                    "unit": "n/a"
                }
            },
            "pep_status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the investor is flagged as a Politically Exposed Person. Renamed directly from bronze eFront investoraccount.PEP_Status.",
                    "long_name": "PEP Status",
                    "unit": "n/a"
                }
            },
            "risk_level": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The compliance risk rating assigned to the investor. Renamed directly from bronze eFront investoraccount.Risk_Level.",
                    "long_name": "Investor Risk Level",
                    "unit": "n/a"
                }
            },
            "investor_account_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investor account. Renamed directly from bronze eFront investoraccount.Investor_Account_IQId; part of this table's composite match key.",
                    "long_name": "Investor Account GUID",
                    "unit": "n/a"
                }
            },
            "investor_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the investor. Renamed directly from bronze eFront investoraccount.Investor_IQId; part of this table's composite match key.",
                    "long_name": "Investor GUID",
                    "unit": "n/a"
                }
            },
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund the account belongs to. Renamed directly from bronze eFront investoraccount.Fund_IQId; part of this table's composite match key and used to join fundoperations records to this account.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor account record was created. Renamed directly from bronze eFront investoraccount.Created_on.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The user or system that last modified the investor account record. Renamed directly from bronze eFront investoraccount.Modified_by.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "modified_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor account record was last modified. Renamed directly from bronze eFront investoraccount.Modified_on.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when the underlying bronze record was ingested into the Databricks Data Lake, passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "investment_portfolio_track_record_reporting": {
        "table": {
            "comment": "Silver layer table providing a consolidated, reporting-ready view of investment portfolios and their underlying assets. It aggregates core attributes (countries, technologies, lifecycle phases), determines top-level reporting classifications (e.g., 'Multi-Country', 'Multi-Tech Renewables'), retrieves key lifecycle and ownership milestone dates (COD, Acquisition, Lockbox), and limits the view to the latest active state per portfolio."
        },
        "columns": {
            "fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The short name of the fund associated with the investment portfolio.",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "fund_legal_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full legal name of the fund, joined from the fund dimension table.",
                    "long_name": "Fund Legal Name",
                    "unit": "n/a"
                }
            },
            "investment_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The display name of the investment portfolio.",
                    "long_name": "Investment Portfolio Name",
                    "unit": "n/a"
                }
            },
            "is_active_portfolio": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "Flag indicating whether the portfolio is currently active, based on CURRENT_DATE falling between its active_from_date and active_to_date.",
                    "long_name": "Is Active Portfolio Flag",
                    "unit": "n/a"
                }
            },
            "inv_pipeline_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The pipeline classification type of the investment portfolio (e.g., Pipeline, Executed).",
                    "long_name": "Investment Pipeline Type",
                    "unit": "n/a"
                }
            },
            "invp_investment_strategy": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The overarching investment strategy mapped to the portfolio.",
                    "long_name": "Investment Strategy",
                    "unit": "n/a"
                }
            },
            "reporting_asset_country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The consolidated geographic reporting location for the portfolio's assets. Applies custom logic (e.g., specific INVP_1132 override for Spain, 'Multi-Country' if spread across multiple distinct countries, or the single country grouping if unified).",
                    "long_name": "Reporting Asset Country",
                    "unit": "n/a"
                }
            },
            "reporting_asset_technology": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The consolidated technology classification for reporting. Will specify combinations (e.g., 'Solar & Battery storage') or fallback to 'Multi-Tech Renewables' if 2 or more technologies are present without specific override logic.",
                    "long_name": "Reporting Asset Technology",
                    "unit": "n/a"
                }
            },
            "latest_ownership_percentage": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The current SPV ownership percentage. Extracted and rounded to 2 decimal places if there is exactly one distinct ownership percentage across the portfolio's assets; NULL otherwise.",
                    "long_name": "Latest Ownership Percentage",
                    "unit": "%"
                }
            },
            "reporting_asset_lifecycle_phase": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The aggregated, prioritized lifecycle phase of the underlying assets for reporting purposes (e.g., Operational, In Construction).",
                    "long_name": "Reporting Asset Lifecycle Phase",
                    "unit": "n/a"
                }
            },
            "reporting_asset_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The consolidated structural asset type classification used for reporting.",
                    "long_name": "Reporting Asset Type",
                    "unit": "n/a"
                }
            },
            "lockbox_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The lockbox date recorded at the investment portfolio level from the eFront system.",
                    "long_name": "Portfolio Lockbox Date",
                    "unit": "n/a"
                }
            },
            "invp_active_from_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The earliest date from which this portfolio configuration became active.",
                    "long_name": "Portfolio Active From Date",
                    "unit": "n/a"
                }
            },
            "invp_active_to_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The date on which this portfolio configuration ceased to be active (NULL or 9999-12-31 if currently active).",
                    "long_name": "Portfolio Active To Date",
                    "unit": "n/a"
                }
            },
            "earliest_asset_cod": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The earliest Commercial Operation Date (COD) across all assets in the portfolio, extracted from lifecycle milestones.",
                    "long_name": "Earliest Asset COD",
                    "unit": "n/a"
                }
            },
            "earliest_asset_lockbox": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The earliest lockbox date across all assets in the portfolio, extracted from ownership milestones.",
                    "long_name": "Earliest Asset Lockbox Date",
                    "unit": "n/a"
                }
            },
            "earliest_asset_acquisition": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The earliest acquisition date across all assets in the portfolio, extracted from ownership milestones.",
                    "long_name": "Earliest Asset Acquisition Date",
                    "unit": "n/a"
                }
            },
            "fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The core internal identifier (GUID) for the fund.",
                    "long_name": "Fund Core ID",
                    "unit": "n/a"
                }
            },
            "investment_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The core internal identifier (GUID) for the investment portfolio.",
                    "long_name": "Investment Portfolio Core ID",
                    "unit": "n/a"
                }
            },
            "asset_country_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "An array of distinct internal system identifiers mapping to the countries of the underlying assets.",
                    "long_name": "Asset Country Core IDs",
                    "unit": "n/a"
                }
            },
            "asset_technology_name_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "An array of distinct internal system identifiers mapping to the technologies of the underlying assets.",
                    "long_name": "Asset Technology IDs",
                    "unit": "n/a"
                }
            },
            "asset_country_group_ids": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": "An array of distinct internal system identifiers mapping to the country groups (e.g., regions) of the underlying assets.",
                    "long_name": "Asset Country Group IDs",
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

    "fundoperations_raw": {
        "table": {
            "comment": "Silver layer table of individual eFront fund operation transactions (calls, distributions, NAV updates, commitments), deduplicated to one record per Transaction_Investor_IQId and enriched with the fund's Core Data Model (CDM) display/legal name. Excludes 'Top 20' share-class rows and draft transactions. This is a lightweight raw feed used to build the aggregated fundoperations_daily table; amount_fund_curr is a single coalesced value covering multiple different source columns depending on transaction type."
        },
        "columns": {
            "ef_fund_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund. Renamed from bronze eFront fundoperations.Fund_IQId; used to join the CDM fund mapping.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "fund_display_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical short display name in the Core Data Model. Sourced from bronze_fund_dim_names.fund_display_name via the CDM mapping chain (bronze_mapping_fund -> bronze_fund_dim_core -> bronze_fund_dim_names -> bronze_fund_dim_structure_type), filtered to source_system_id = 'SRCE_SYST_1001' with all three CDM dimension records active (END_AT IS NULL) and deduplicated to the most recently updated mapping per fund.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "fund_legal_name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical legal name in the Core Data Model. Sourced from bronze_fund_dim_names.fund_legal_name via the same CDM mapping chain as fund_display_name.",
                    "long_name": "CDM Fund Legal Name",
                    "unit": "n/a"
                }
            },
            "investor_account": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor account the transaction relates to. Renamed directly from bronze eFront fundoperations.Investor_Account.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "share": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The share class of the transaction. Renamed directly from bronze eFront fundoperations.Share; rows where this equals 'Top 20' are excluded from this table entirely as they represent a summary/duplicate share class rather than a real transaction.",
                    "long_name": "Share Class",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific fund operation type (e.g., MF: Call, MF: Distribution, MF: Net Asset Value). Renamed directly from bronze eFront fundoperations.Type; drives which amount is populated and how fundoperations_daily aggregates the row.",
                    "long_name": "Operation Type",
                    "unit": "n/a"
                }
            },
            "effective_date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The effective date of the fund operation, formatted as yyyy-MM-dd. Derived from bronze eFront fundoperations.Effective_Date via date_format().",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "fund_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The currency of the fund operation amount. Renamed directly from bronze eFront fundoperations.Currency.",
                    "long_name": "Fund Currency",
                    "unit": "n/a"
                }
            },
            "units": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The number of fund units/shares involved in the transaction. Renamed directly from bronze eFront fundoperations.Quantity.",
                    "long_name": "Unit Quantity",
                    "unit": "units"
                }
            },
            "draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the transaction is in a draft/uncommitted state. Renamed directly from bronze eFront fundoperations.Draft; rows where this equals 'True' are excluded from this table entirely.",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            },
            "redrawable_amount_fund_curr": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The redrawable commitment amount in fund currency (0 if not applicable to this operation type). COALESCE(Redrawable_amount__fund_curr_, 0) from bronze eFront fundoperations.",
                    "long_name": "Redrawable Amount",
                    "unit": "fund currency"
                }
            },
            "amount_fund_curr": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The transaction amount in fund currency. A single coalesced value that picks whichever of Amount__fund_curr_, Committed_amount__fund_curr_, Valuation__fund_curr_, or FundExpired_Original_Commitment is non-zero (in that priority order), defaulting to 0 if all are zero/null, since only one of those source columns is populated depending on the operation type.",
                    "long_name": "Operation Amount",
                    "unit": "fund currency"
                }
            },
            "unit_price": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The per-unit price applied to the transaction. Renamed directly from bronze eFront fundoperations._Unit_Price.",
                    "long_name": "Unit Price",
                    "unit": "fund currency"
                }
            },
            "created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the fund operation record was created in the source system. Renamed directly from bronze eFront fundoperations.Created_on.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the fund operation record. Renamed directly from bronze eFront fundoperations.Modified_by.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the fund operation record. Renamed directly from bronze eFront fundoperations.Modified_on.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when the underlying bronze record was ingested into the Databricks Data Lake, passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_daily": {
        "table": {
            "comment": "Silver layer daily fact table of fund-level capital activity (commitments, calls, distributions, NAV), aggregated from fundoperations_raw's underlying source (fundoperations, fund, investoraccount) grouped by fund/investor/currency/date, enriched with CDM fund identity and structure."
        },
        "columns": {
            "Ef_Fund_Id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund. Sourced from bronze eFront fundoperations.Fund_IQId; part of this table's composite match key.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "Ef_Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The eFront short name of the fund. Sourced from bronze eFront fund.Short_Name via a LEFT JOIN on Fund_IQId.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Ef_Fund_Long": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full eFront fund name as recorded on the fund operation record itself. Sourced from bronze eFront fundoperations.Fund.",
                    "long_name": "eFront Fund Full Name",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical short display name in the Core Data Model. Sourced from bronze_fund_dim_names.fund_display_name via the CDM mapping chain (bronze_mapping_fund -> bronze_fund_dim_core -> bronze_fund_dim_names), restricted to eFront's CDM source system and active (END_AT IS NULL) dimension records.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's structural classification (e.g., Master Fund, Feeder Fund) as recorded in the Core Data Model. Sourced from bronze_fund_dim_structure_type.fund_structure_type_name via the same CDM mapping chain.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is currently active or has ended. Computed as 'Active' when bronze_fund_dim_core.fund_end_date IS NULL, else 'Inactive'.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor account the aggregated activity relates to. Sourced from bronze eFront fundoperations.Investor_Account; part of this table's composite match key.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's classification (e.g., Institutional). Sourced from bronze eFront investoraccount.Investor_Type via a LEFT JOIN on Investor_Account + Fund_IQId, except when it is blank AND the investor_account name itself matches another fund's Short_Name — in that self-investment case it is set to the literal 'Feeder Fund'.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's country. Sourced from bronze eFront investoraccount.Country via the same join as Investor_Type, except when it is blank AND the investor_account matches another fund's Short_Name — in that self-investment case it is set to the literal 'N/A'.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Effective_Date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The date of the fund operation, formatted as yyyy-MM-dd. Sourced from bronze eFront fundoperations.Effective_Date; part of this table's composite match key.",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The currency of the fund operation amounts on this row. Sourced from bronze eFront fundoperations.Currency; part of this table's composite match key.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net capital commitment activity for the fund/investor/currency/date grouping. SUM(Committed_amount + Redrawable_amount - Expired_commitment) across all matching fund operation rows, where each term is COALESCE'd to 0 from the corresponding bronze amount column when the operation type doesn't populate it.",
                    "long_name": "Total Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called from investors for the grouping. SUM of Amount__fund_curr_ restricted to rows where Type IN ('MF: Call', 'MF: Return Of Call (Negative Call)'), else 0.",
                    "long_name": "Total Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed to investors for the grouping. SUM of Amount__fund_curr_ restricted to rows where Type = 'MF: Distribution', else 0.",
                    "long_name": "Total Distribution",
                    "unit": "transaction currency"
                }
            },
            "NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The Net Asset Value reported for the grouping. SUM of Valuation__fund_curr_ restricted to rows where Type = 'MF: Net Asset Value', else 0.",
                    "long_name": "Net Asset Value",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments to date. SUM(Commitments) OVER a window partitioned by Fund_IQId/Transaction_Currency/Investor_Account/Investor_Type/Investor_Country ordered by Effective_Date.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called to date, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Remaining_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unfunded commitment balance remaining. Calculated as Cumulative_Commitments minus Cumulative_Called.",
                    "long_name": "Remaining Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution to date, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The most recent bronze ingestion timestamp among the underlying fund operation rows contributing to this grouping (MAX(datalake_ingestion_timestamp)).",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_monthly": {
        "table": {
            "comment": "Silver layer monthly-calendarised fact table derived from fundoperations_daily. Aggregates daily activity to calendar month, then builds an explicit month-by-month calendar spine per fund/investor/currency combination — spanning from that combination's first active month to the latest month observed across ALL combinations — so every combination reports every month even when no activity occurred, with flow columns (Commitments/Called/Distribution) zero-filled and NAV forward-filled from the last known value."
        },
        "columns": {
            "Ef_Fund_Id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "Ef_Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The eFront short name of the fund, carried through unchanged from fundoperations_daily.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Ef_Fund_Long": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full eFront fund name, carried through unchanged from fundoperations_daily.",
                    "long_name": "eFront Fund Full Name",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor account, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's country, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Year_Month": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar month this row represents, formatted as yyyy-MM. Generated either from date_trunc('month', Effective_Date) on real activity, or as a spine month (EXPLODE(SEQUENCE(...))) between the combination's first active month and the latest month observed across all fund/investor/currency combinations.",
                    "long_name": "Reporting Year-Month",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the month. SUM(Commitments) from fundoperations_daily rows in that month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called during the month. SUM(Called) from fundoperations_daily rows in that month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed during the month. SUM(Distribution) from fundoperations_daily rows in that month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Distribution",
                    "unit": "transaction currency"
                }
            },
            "NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net Asset Value at month end. MAX(NAV) from fundoperations_daily rows in that month if any exist; otherwise forward-filled from the most recent prior month's NAV via last(NAV, true) OVER an ordered window, so a month with no new NAV report retains the last known value rather than showing zero or blank.",
                    "long_name": "Monthly Net Asset Value",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised monthly series, partitioned by Ef_Fund_Id/Transaction_Currency/Investor/Investor_Type/Investor_Country ordered by Year_Month.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Remaining_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unfunded commitment balance remaining. Calculated as Cumulative_Commitments minus Cumulative_Called.",
                    "long_name": "Remaining Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_quarterly": {
        "table": {
            "comment": "Silver layer quarterly-calendarised fact table derived from fundoperations_daily, following the same calendar-spine logic as fundoperations_monthly but truncated to calendar quarters — every fund/investor/currency combination reports every quarter from its first active quarter to the latest quarter observed across all combinations, with flow columns zero-filled and NAV forward-filled."
        },
        "columns": {
            "Ef_Fund_Id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "Ef_Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The eFront short name of the fund, carried through unchanged from fundoperations_daily.",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Ef_Fund_Long": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full eFront fund name, carried through unchanged from fundoperations_daily.",
                    "long_name": "eFront Fund Full Name",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor account, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's country, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Year_Quarter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar quarter this row represents, formatted as 'yyyy-Q<n>' via CONCAT(date_format(q_start, 'yyyy'), '-Q', QUARTER(q_start)). Generated either from date_trunc('quarter', Effective_Date) on real activity, or as a spine quarter between the combination's first active quarter and the latest quarter observed across all combinations.",
                    "long_name": "Reporting Year-Quarter",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily and used as part of the calendar spine's grouping key.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the quarter. SUM(Commitments) from fundoperations_daily rows in that quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called during the quarter. SUM(Called) from fundoperations_daily rows in that quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed during the quarter. SUM(Distribution) from fundoperations_daily rows in that quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Distribution",
                    "unit": "transaction currency"
                }
            },
            "Remaining_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unfunded commitment balance remaining. Calculated as Cumulative_Commitments minus Cumulative_Called.",
                    "long_name": "Remaining Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised quarterly series, partitioned by Ef_Fund_Id/Transaction_Currency/Investor/Investor_Type/Investor_Country ordered by quarter start date.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net Asset Value at quarter end, forward-filled from the most recent prior quarter's NAV via LAST_VALUE(NAV, TRUE) OVER an ordered window, so a quarter with no new NAV report retains the last known value.",
                    "long_name": "Quarterly Net Asset Value",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_by_investor": {
        "table": {
            "comment": "Silver layer fact table of fund operation activity re-cast from a fund-centric to an investor-centric view. Reuses fundoperations_daily's per-fund/investor/date rows unchanged but recomputes cumulative totals and remaining commitments partitioned only by Investor/Investor_Type/Investor_Country/Transaction_Currency (i.e. summed across all of that investor's funds), ordered by Effective_Date then Ef_Fund_Id."
        },
        "columns": {
            "Ef_Fund_Id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique eFront system identifier (GUID) for the fund the row's activity belongs to, carried through unchanged from fundoperations_daily.",
                    "long_name": "eFront Fund GUID",
                    "unit": "n/a"
                }
            },
            "ef_Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's short eFront name, carried through unchanged from fundoperations_daily (distinct from the CDM display name held in the Fund column).",
                    "long_name": "eFront Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through unchanged from fundoperations_daily.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor account, carried through unchanged from fundoperations_daily and used as the primary partition for this table's cumulative totals.",
                    "long_name": "Investor Account Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's classification, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor's country, carried through unchanged from fundoperations_daily.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Effective_Date": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The date of the fund operation, carried through unchanged from fundoperations_daily.",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through unchanged from fundoperations_daily and used as part of this table's cumulative-totals partition.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for this fund/investor/date row, carried through unchanged from fundoperations_daily.",
                    "long_name": "Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for this fund/investor/date row, carried through unchanged from fundoperations_daily.",
                    "long_name": "Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed for this fund/investor/date row, carried through unchanged from fundoperations_daily.",
                    "long_name": "Distribution",
                    "unit": "transaction currency"
                }
            },
            "NAV": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net Asset Value for this fund/investor/date row, carried through unchanged from fundoperations_daily.",
                    "long_name": "Net Asset Value",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across ALL of the investor's funds. SUM(Commitments) OVER a window partitioned by Investor/Investor_Type/Investor_Country/Transaction_Currency ordered by Effective_Date then Ef_Fund_Id — i.e. summed across funds, unlike fundoperations_daily's per-fund cumulative.",
                    "long_name": "Cumulative Commitments (All Funds)",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across all of the investor's funds, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called (All Funds)",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across all of the investor's funds, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution (All Funds)",
                    "unit": "transaction currency"
                }
            },
            "Remaining_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unfunded commitment balance remaining across all of the investor's funds. Calculated as Cumulative_Commitments minus Cumulative_Called.",
                    "long_name": "Remaining Commitments (All Funds)",
                    "unit": "transaction currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the underlying fundoperations_daily record was ingested into the Data Lake, carried through unchanged.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_by_investor_type_monthly": {
        "table": {
            "comment": "Silver layer monthly-calendarised fact table aggregating fundoperations_daily by Fund/Fund_Structure_Type/Fund_Status/Transaction_Currency/Investor_Type (COALESCE'd to 'Unknown' when blank), collapsing across individual investor accounts. Builds a per-slice calendar spine from that slice's first active month to the latest month observed across ALL slices, zero-filling months with no activity."
        },
        "columns": {
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily; part of this table's grouping key and calendar spine partition.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor classification this row aggregates, from fundoperations_daily's Investor_Type but with blank/null values replaced by the literal 'Unknown' via COALESCE, so every investor type — including unclassified investors — gets its own row.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Year_Month": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar month this row represents, formatted as yyyy-MM (TRUNC(TO_DATE(Effective_Date), 'MM')). Generated either from real activity in that month, or as a spine month between the slice's first active month and the latest month observed across all Fund/Transaction_Currency/Investor_Type slices.",
                    "long_name": "Reporting Year-Month",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the slice/month. SUM(Commitments) from fundoperations_daily rows across all investor accounts in that Investor_Type for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for the slice/month. SUM(Called) from fundoperations_daily rows across all investor accounts in that Investor_Type for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed for the slice/month. SUM(Distribution) from fundoperations_daily rows across all investor accounts in that Investor_Type for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Distribution",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised monthly series, partitioned by Fund/Transaction_Currency/Investor_Type ordered by month.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_by_investor_country_monthly": {
        "table": {
            "comment": "Silver layer monthly-calendarised fact table aggregating fundoperations_daily by Fund/Fund_Structure_Type/Fund_Status/Transaction_Currency/Investor_Country (COALESCE'd to 'Unknown' when blank), collapsing across individual investor accounts. Identical structure and calendarisation logic to fundoperations_by_investor_type_monthly, sliced by Investor_Country instead of Investor_Type."
        },
        "columns": {
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily; part of this table's grouping key and calendar spine partition.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor country this row aggregates, from fundoperations_daily's Investor_Country but with blank/null values replaced by the literal 'Unknown' via COALESCE, so every country — including unclassified investors — gets its own row.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Year_Month": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar month this row represents, formatted as yyyy-MM. Generated either from real activity in that month, or as a spine month between the slice's first active month and the latest month observed across all Fund/Transaction_Currency/Investor_Country slices.",
                    "long_name": "Reporting Year-Month",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the slice/month. SUM(Commitments) from fundoperations_daily rows across all investor accounts in that Investor_Country for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for the slice/month. SUM(Called) from fundoperations_daily rows across all investor accounts in that Investor_Country for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed for the slice/month. SUM(Distribution) from fundoperations_daily rows across all investor accounts in that Investor_Country for the month, or 0 if the calendar spine month has no matching activity.",
                    "long_name": "Monthly Distribution",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised monthly series, partitioned by Fund/Transaction_Currency/Investor_Country ordered by month.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised monthly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_by_investor_type_quarterly": {
        "table": {
            "comment": "Silver layer quarterly-calendarised fact table aggregating fundoperations_daily by Fund/Fund_Structure_Type/Fund_Status/Transaction_Currency/Investor_Type (COALESCE'd to 'Unknown' when blank). Same aggregation and Unknown-fallback logic as fundoperations_by_investor_type_monthly, calendarised to calendar quarters instead of months."
        },
        "columns": {
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily; part of this table's grouping key and calendar spine partition.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor classification this row aggregates, from fundoperations_daily's Investor_Type but with blank/null values replaced by the literal 'Unknown' via COALESCE.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Year_Quarter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar quarter this row represents, formatted as 'yyyy-Q<n>'. Generated either from real activity in that quarter, or as a spine quarter between the slice's first active quarter and the latest quarter observed across all Fund/Transaction_Currency/Investor_Type slices.",
                    "long_name": "Reporting Year-Quarter",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the slice/quarter. SUM(Commitments) from fundoperations_daily rows across all investor accounts in that Investor_Type for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for the slice/quarter. SUM(Called) from fundoperations_daily rows across all investor accounts in that Investor_Type for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed for the slice/quarter. SUM(Distribution) from fundoperations_daily rows across all investor accounts in that Investor_Type for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Distribution",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised quarterly series, partitioned by Fund/Transaction_Currency/Investor_Type ordered by quarter.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fundoperations_by_investor_country_quarterly": {
        "table": {
            "comment": "Silver layer quarterly-calendarised fact table aggregating fundoperations_daily by Fund/Fund_Structure_Type/Fund_Status/Transaction_Currency/Investor_Country (COALESCE'd to 'Unknown' when blank). Same aggregation and Unknown-fallback logic as fundoperations_by_investor_country_monthly, calendarised to calendar quarters instead of months."
        },
        "columns": {
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's canonical CDM display name, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Display Name",
                    "unit": "n/a"
                }
            },
            "Fund_Structure_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The fund's CDM structural classification, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Structure Type",
                    "unit": "n/a"
                }
            },
            "Fund_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Whether the fund's CDM fund-core record is Active or Inactive, carried through from fundoperations_daily; part of this table's grouping key.",
                    "long_name": "CDM Fund Active Status",
                    "unit": "n/a"
                }
            },
            "Transaction_Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The transaction currency, carried through from fundoperations_daily; part of this table's grouping key and calendar spine partition.",
                    "long_name": "Transaction Currency",
                    "unit": "n/a"
                }
            },
            "Investor_Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor country this row aggregates, from fundoperations_daily's Investor_Country but with blank/null values replaced by the literal 'Unknown' via COALESCE.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Year_Quarter": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The calendar quarter this row represents, formatted as 'yyyy-Q<n>'. Generated either from real activity in that quarter, or as a spine quarter between the slice's first active quarter and the latest quarter observed across all Fund/Transaction_Currency/Investor_Country slices.",
                    "long_name": "Reporting Year-Quarter",
                    "unit": "n/a"
                }
            },
            "Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net commitment activity for the slice/quarter. SUM(Commitments) from fundoperations_daily rows across all investor accounts in that Investor_Country for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Commitments",
                    "unit": "transaction currency"
                }
            },
            "Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital called for the slice/quarter. SUM(Called) from fundoperations_daily rows across all investor accounts in that Investor_Country for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Called",
                    "unit": "transaction currency"
                }
            },
            "Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Capital distributed for the slice/quarter. SUM(Distribution) from fundoperations_daily rows across all investor accounts in that Investor_Country for the quarter, or 0 if the calendar spine quarter has no matching activity.",
                    "long_name": "Quarterly Distribution",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Commitments": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Commitments across the calendarised quarterly series, partitioned by Fund/Transaction_Currency/Investor_Country ordered by quarter.",
                    "long_name": "Cumulative Commitments",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Called": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Called across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Called",
                    "unit": "transaction currency"
                }
            },
            "Cumulative_Distribution": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Running total of Distribution across the calendarised quarterly series, using the same partition/order as Cumulative_Commitments.",
                    "long_name": "Cumulative Distribution",
                    "unit": "transaction currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "aggregated_asset_transactions": {
        "table": {
            "comment": "Silver layer table of asset-level cashflows and valuations, aggregated from silver transactions and transactions_paid. Classifies each transaction into a cashflow_type (Injections/Distributions/Sales) via an inline lookup, excludes commitments/valuations/interest already captured elsewhere, and separately synthesizes a quarterly 'Portfolio NAV' row per portfolio (Equity valuation plus either Loan valuation or, if absent, cumulative shareholder-loan balance) and an 'External Debt Valuation' row for Loan valuations flagged as OEGEN external asset debt."
        },
        "columns": {
            "company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investee company the row relates to. Sourced directly from silver transactions/transactions_paid.company for cashflow rows; set to the literal 'Multiple/Mixed' for the synthesized Portfolio NAV rows, which aggregate across all companies in the portfolio.",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "company_investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investor entity for the row. Sourced directly from silver transactions/transactions_paid.company_investor for cashflow rows; set to the literal 'Multiple/Mixed' for the synthesized Portfolio NAV rows.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "investment_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The investment portfolio/project the row relates to. Sourced directly from silver transactions/transactions_paid.investment_portfolio.",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The financial instrument involved. Sourced directly from silver transactions/transactions_paid.instrument for cashflow rows; set to the literal 'Portfolio NAV' for the synthesized valuation rows.",
                    "long_name": "Instrument",
                    "unit": "n/a"
                }
            },
            "type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The original transaction type. Sourced directly from silver transactions/transactions_paid.type for cashflow rows; set to the literal 'Portfolio Valuation' for synthesized NAV rows, or carried through as the LN valuation's type for external asset debt rows.",
                    "long_name": "Transaction Type",
                    "unit": "n/a"
                }
            },
            "cashflow_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The reporting classification of the cashflow: 'Injections', 'Distributions', or 'Sales' for eligible transactions (matched by an inline lookup against the source type, e.g. 'EQ - Sale/Redemption' -> 'Sales'), 'Portfolio Valuation' for synthesized Portfolio NAV rows, or 'External Debt Valuation' for external asset debt rows.",
                    "long_name": "Cashflow Classification",
                    "unit": "n/a"
                }
            },
            "report_date": {
                "schema": {"data_type": "DATE"},
                "description": {
                    "comment": "The reporting period this row is bucketed into. For cashflow rows this is LAST_DAY(effective_date) (month-end); for Portfolio NAV / external debt rows this is snapped to the nearest calendar quarter-end via LAST_DAY(DATE_TRUNC('quarter', effective_date) + INTERVAL '2 months').",
                    "long_name": "Report Period End Date",
                    "unit": "n/a"
                }
            },
            "instrument_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The currency of the instrument/amount. Sourced directly from silver transactions/transactions_paid.instrument_currency.",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "amount_instrument": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The amount for the row in instrument currency. For cashflow rows, SUM(amount_instrument) grouped by company/company_investor/portfolio/instrument/type/cashflow_type/report_date/currency across the unioned transactions and transactions_paid (excluding commitments, valuations, and cash/capitalised interest, and requiring oegen_type IS NULL, aum_reporting = TRUE, exclude_transaction_costs not true, and draft not true). For Portfolio NAV rows, COALESCE(EQ valuation, 0) plus the LN valuation if one exists for that quarter, else the cumulative shareholder-loan balance (sign-flipped) as of that quarter-end. For External Debt Valuation rows, SUM(valuation_instrument) from LN valuations where oegen_type = 'external asset debt'. Rows where this is NULL after all the above are dropped entirely.",
                    "long_name": "Instrument Amount",
                    "unit": "instrument currency"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fees_company": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront fees_company: itemized fee, cost, and expense records associated with specific investee companies or SPVs. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added."
        },
        "columns": {
            "Comment": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "A description or specific detail regarding the nature of the fee. Passed through unchanged from bronze eFront fees_company.Comment.",
                    "long_name": "Fee Comment",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the fee record was originally created in the source system. Passed through unchanged from bronze.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Draft": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating if the fee record is in a draft or uncommitted state (e.g., True/False). Passed through unchanged from bronze.",
                    "long_name": "Draft Status",
                    "unit": "n/a"
                }
            },
            "Effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The date on which the fee or cost becomes effective or is applied. Passed through unchanged from bronze.",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "Entity": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legal name of the entity, company, or SPV associated with the fee. Passed through unchanged from bronze.",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fee record from the source API. Passed through unchanged from bronze.",
                    "long_name": "Fee Record GUID",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the fee record. Passed through unchanged from bronze.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the fee record in the source system. Passed through unchanged from bronze.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Net_Amount_Due": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total net financial amount due for the fee or cost. Passed through unchanged from bronze.",
                    "long_name": "Net Amount Due",
                    "unit": "currency"
                }
            },
            "Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The classification or category of the fee (e.g., Transaction Fee, W&I cost). Passed through unchanged from bronze.",
                    "long_name": "Fee Type",
                    "unit": "n/a"
                }
            },
            "Prepayment": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating whether the fee was paid in advance as a prepayment. Passed through unchanged from bronze.",
                    "long_name": "Prepayment Flag",
                    "unit": "n/a"
                }
            },
            "Release": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Flag indicating whether the fee record is associated with a release of funds or liabilities. Passed through unchanged from bronze.",
                    "long_name": "Release Flag",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "fees_fund": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront fees_fund: itemized fee and allocation records at the fund and investor level. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added."
        },
        "columns": {
            "Allocation_Rule": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The specific rule or period description determining how the fee is allocated (e.g., OEDP Mgmt Fee Jun-22). Passed through unchanged from bronze eFront fees_fund.Allocation_Rule.",
                    "long_name": "Allocation Rule",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating when the fund fee record was originally created in the source system. Passed through unchanged from bronze.",
                    "long_name": "Source Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Effective_date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The date on which the fund fee or allocation becomes effective. Passed through unchanged from bronze.",
                    "long_name": "Effective Date",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The legal name of the fund associated with the fee. Passed through unchanged from bronze.",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Gross_Amount_Due": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The gross financial amount due for the fee prior to any deductions. Passed through unchanged from bronze.",
                    "long_name": "Gross Amount Due",
                    "unit": "fund currency"
                }
            },
            "IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fund fee record from the source API. Passed through unchanged from bronze.",
                    "long_name": "Fee Record GUID",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name or short code of the investor to whom the fee portion is allocated. Passed through unchanged from bronze.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "Investor_Account_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor account identifier from eFront API. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Investor Account ID",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The username or system ID that last modified the fund fee record. Passed through unchanged from bronze.",
                    "long_name": "Last Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp indicating the last modification of the fund fee record in the source system. Passed through unchanged from bronze.",
                    "long_name": "Source Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Short_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The short name or ticker used internally to identify the fund. Passed through unchanged from bronze.",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The classification or category of the fee (e.g., Management Fee). Passed through unchanged from bronze.",
                    "long_name": "Fee Type",
                    "unit": "n/a"
                }
            },
            "Units": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The apportioned units or exact amount allocated to the specific investor. Passed through unchanged from bronze.",
                    "long_name": "Allocated Units / Amount",
                    "unit": "n/a"
                }
            },
            "Net_Amount_Due": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The total net financial amount due for the fund fee after relevant deductions. Passed through unchanged from bronze.",
                    "long_name": "Net Amount Due",
                    "unit": "fund currency"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "instruments": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront instruments: the financial instruments (equity, loans, etc.) issued by investee companies. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added."
        },
        "columns": {
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The company associated with the instrument. Passed through unchanged from bronze eFront instruments.Company.",
                    "long_name": "Company Name",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier from eFront API. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Company ID",
                    "unit": "n/a"
                }
            },
            "Complete_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Complete name or description of the instrument. Passed through unchanged from bronze.",
                    "long_name": "Instrument Complete Name",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the instrument record was created. Passed through unchanged from bronze.",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency in which the instrument is denominated. Passed through unchanged from bronze.",
                    "long_name": "Instrument Currency",
                    "unit": "n/a"
                }
            },
            "Description": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Description or details about the instrument. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Instrument Description",
                    "unit": "n/a"
                }
            },
            "Instrument": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type or identifier of the instrument (e.g., stock, bond). Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Instrument Type",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the instruments record. Passed through unchanged from bronze.",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the instruments record was last modified. Passed through unchanged from bronze.",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "OEGEN_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Categorises the structural level of external debt facilities associated with this instrument, differentiating between debt raised for a specific underlying project versus debt raised at the overarching fund level. Passed through unchanged from bronze; used by downstream silver_aggregated_asset_transactions to isolate external asset debt and to exclude asset-debt rows from ordinary cashflow/valuation aggregation.",
                    "long_name": "OEGEN Type",
                    "unit": "n/a"
                }
            },
            "Portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Portfolio to which the instrument belongs. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Portfolio Name",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "investor": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront investor: master data for investors. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added. Note this table is distinct from silver investoraccount, which represents an investor's specific holding account within a fund."
        },
        "columns": {
            "Country": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the investor is registered or domiciled. Passed through unchanged from bronze eFront investor.Country.",
                    "long_name": "Investor Country",
                    "unit": "n/a"
                }
            },
            "Created_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor record was created. Passed through unchanged from bronze.",
                    "long_name": "Record Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "Currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used by the investor for financial reporting. Passed through unchanged from bronze.",
                    "long_name": "Investor Currency",
                    "unit": "n/a"
                }
            },
            "Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the investor. Passed through unchanged from bronze.",
                    "long_name": "Investor Name",
                    "unit": "n/a"
                }
            },
            "Investor_Type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Classification of the investor (e.g., individual, institutional). Passed through unchanged from bronze.",
                    "long_name": "Investor Type",
                    "unit": "n/a"
                }
            },
            "Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investor identifier from eFront API. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Investor ID",
                    "unit": "n/a"
                }
            },
            "Legal_Name": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Legal name of the investor. Passed through unchanged from bronze.",
                    "long_name": "Investor Legal Name",
                    "unit": "n/a"
                }
            },
            "Modified_by": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "User or system that last modified the investor record. Passed through unchanged from bronze.",
                    "long_name": "Modified By",
                    "unit": "n/a"
                }
            },
            "Modified_on": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the investor record was last modified. Passed through unchanged from bronze.",
                    "long_name": "Record Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "Risk_Level": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Risk level associated with the investor. Passed through unchanged from bronze.",
                    "long_name": "Investor Risk Level",
                    "unit": "n/a"
                }
            },
            "Tax_Domicile__Country_": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Country where the investor is domiciled for tax purposes. Passed through unchanged from bronze.",
                    "long_name": "Investor Tax Domicile Country",
                    "unit": "n/a"
                }
            },
            "AML_Compliance_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Anti-money laundering compliance status of the investor. Passed through unchanged from bronze.",
                    "long_name": "AML Compliance Status",
                    "unit": "n/a"
                }
            },
            "PEP_Status": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Politically Exposed Person status of the investor. Passed through unchanged from bronze.",
                    "long_name": "PEP Status",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "ownership": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront ownership: mapping of ownership stakes, investment relationships, and holding structures between funds, investing entities, and investee companies. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added."
        },
        "columns": {
            "Company": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full registered name of the investee company or asset being held. Passed through unchanged from bronze eFront ownership.Company; part of this table's match key.",
                    "long_name": "Investee Company Name",
                    "unit": "n/a"
                }
            },
            "Company_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investee company from eFront. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Investee Company GUID",
                    "unit": "n/a"
                }
            },
            "Company_Investor": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The name of the investing company, SPV, or intermediate holding entity. Passed through unchanged from bronze.",
                    "long_name": "Investor Company Name",
                    "unit": "n/a"
                }
            },
            "Company_Investor_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the investing company from eFront. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Investor Company GUID",
                    "unit": "n/a"
                }
            },
            "Fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The full name of the fund sitting at the top of this ownership relationship. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Fund Name",
                    "unit": "n/a"
                }
            },
            "Fund_IQId": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The unique system identifier (GUID) for the fund from eFront. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Fund GUID",
                    "unit": "n/a"
                }
            },
            "Investee_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary currency used by the investee company (e.g., GBP, USD). Passed through unchanged from bronze.",
                    "long_name": "Investee Currency",
                    "unit": "n/a"
                }
            },
            "Investment_Details": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name, grouping, or categorical details identifying the investment (e.g., Fern, ORIT). Passed through unchanged from bronze.",
                    "long_name": "Investment Details / Grouping",
                    "unit": "n/a"
                }
            },
            "Investor_currency": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "The primary currency used by the investing entity. Passed through unchanged from bronze.",
                    "long_name": "Investor Currency",
                    "unit": "n/a"
                }
            },
            "AUM_Reporting": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Indicates whether this specific investment relationship is included in Assets Under Management reporting. Passed through unchanged from bronze.",
                    "long_name": "AUM Reporting Flag",
                    "unit": "n/a"
                }
            },
            "perc_Stake": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "The percentage of ownership or equity stake held by the investor in the investee. Passed through unchanged from bronze.",
                    "long_name": "Ownership Stake Percentage",
                    "unit": "%"
                }
            },
            "First_Investment_Date": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact date when the initial investment or acquisition was made into the investee company. Passed through unchanged from bronze.",
                    "long_name": "First Investment Date",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The exact system timestamp when this record was ingested into the Databricks Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "datapointsexport": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront datapointsexport: a generic export of individual, ad-hoc reported data points across various entities and categories. No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added."
        },
        "columns": {
            "CATEGORY_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Category name associated with the data point. Passed through unchanged from bronze eFront datapointsexport.CATEGORY_NAME; part of this table's match key.",
                    "long_name": "Data Category",
                    "unit": "n/a"
                }
            },
            "CREATIONDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the data point was created. Passed through unchanged from bronze.",
                    "long_name": "Data Point Creation Timestamp",
                    "unit": "n/a"
                }
            },
            "CURRENCY": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Currency used for the data point. Passed through unchanged from bronze.",
                    "long_name": "Currency Type",
                    "unit": "n/a"
                }
            },
            "DATAPOINT_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the data point. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Data Point Name",
                    "unit": "n/a"
                }
            },
            "ENTITY": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier for the entity associated with the data point. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Entity ID",
                    "unit": "n/a"
                }
            },
            "ENTITY_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the entity associated with the data point. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Entity Name",
                    "unit": "n/a"
                }
            },
            "ENTITY_TYPE": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Type of the entity (e.g., company, fund, etc.). Passed through unchanged from bronze.",
                    "long_name": "Entity Type",
                    "unit": "n/a"
                }
            },
            "FILENAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Name of the file containing the data point. Passed through unchanged from bronze.",
                    "long_name": "File Name",
                    "unit": "n/a"
                }
            },
            "MODIFICATIONDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the data point was last modified. Passed through unchanged from bronze.",
                    "long_name": "Data Point Modification Timestamp",
                    "unit": "n/a"
                }
            },
            "PERIOD": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Period associated with the data point. Passed through unchanged from bronze.",
                    "long_name": "Reporting Period",
                    "unit": "n/a"
                }
            },
            "REFDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Reference date for the data point. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Reference Date",
                    "unit": "n/a"
                }
            },
            "REGION": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Geographic region related to the data point. Passed through unchanged from bronze.",
                    "long_name": "Region Name",
                    "unit": "n/a"
                }
            },
            "REPORTINGDATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date on which the data point was reported. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Reporting Date",
                    "unit": "n/a"
                }
            },
            "REVISIONNUMBER": {
                "schema": {"data_type": "INT"},
                "description": {
                    "comment": "Revision number of the data point. Passed through unchanged from bronze; part of this table's match key.",
                    "long_name": "Revision Number",
                    "unit": "n/a"
                }
            },
            "SCENARIO": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Scenario associated with the data point (e.g., actual, forecast). Passed through unchanged from bronze.",
                    "long_name": "Scenario Type",
                    "unit": "n/a"
                }
            },
            "TEMPLATE_NAME": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Template name associated with the data point. Passed through unchanged from bronze.",
                    "long_name": "Template Name",
                    "unit": "n/a"
                }
            },
            "VALUEMEMO": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Memo or notes describing the value of the data point. Passed through unchanged from bronze.",
                    "long_name": "Value Memo",
                    "unit": "n/a"
                }
            },
            "VALUENUM": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Numerical value of the data point. Passed through unchanged from bronze.",
                    "long_name": "Data Point Value",
                    "unit": "n/a"
                }
            },
            "VALUESTRING": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "String value representing the data point. Passed through unchanged from bronze.",
                    "long_name": "String Data Point Value",
                    "unit": "n/a"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "row_seq": {
                "schema": {"data_type": "INT"},
                "description": {
                    "comment": "De-duplication rank within each (CATEGORY_NAME, CREATIONDATE, DATAPOINT_NAME, ENTITY, REFDATE) group. Calculated as ROW_NUMBER() OVER (PARTITION BY CATEGORY_NAME, CREATIONDATE, DATAPOINT_NAME, ENTITY, REFDATE ORDER BY VALUENUM, VALUESTRING, datalake_ingestion_timestamp); row_seq = 1 identifies the row the uniqueness expectation treats as canonical when duplicates exist in bronze.",
                    "long_name": "Duplicate Row Sequence",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },

    "track_record_fund_raw": {
        "table": {
            "comment": "Silver layer validated passthrough of bronze eFront track_record_fund: raw, un-restated track record performance metrics at the fund level (yields, IRRs, MOIC, DPI). No joins, renames, or aggregation are applied — this is a straight SELECT * from bronze with a refresh timestamp added. This is the raw feed underlying the calculated silver track_record_fund table."
        },
        "columns": {
            "ANNUALISED_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annualised yield for the fund. Passed through unchanged from bronze eFront track_record_fund.ANNUALISED_YIELD.",
                    "long_name": "Annualised Yield",
                    "unit": "%"
                }
            },
            "ANNUAL_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annual yield for the fund. Passed through unchanged from bronze.",
                    "long_name": "Annual Yield",
                    "unit": "%"
                }
            },
            "DPI": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Distributions to Paid-In Capital ratio. Passed through unchanged from bronze.",
                    "long_name": "DPI",
                    "unit": "n/a"
                }
            },
            "FUND_SHORT": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Short name of the fund. Passed through unchanged from bronze; this table's match key.",
                    "long_name": "Fund Short Name",
                    "unit": "n/a"
                }
            },
            "MOIC": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Multiple on invested capital for the fund. Passed through unchanged from bronze.",
                    "long_name": "MOIC",
                    "unit": "n/a"
                }
            },
            "NET_CAGR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net compound annual growth rate for the fund. Passed through unchanged from bronze.",
                    "long_name": "Net CAGR",
                    "unit": "%"
                }
            },
            "NET_TOTAL_RETURN": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net total return of the fund. Passed through unchanged from bronze.",
                    "long_name": "Net Total Return",
                    "unit": "%"
                }
            },
            "REPORTING_DATE": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Date of the report for the fund. Passed through unchanged from bronze; part of this table's uniqueness key alongside FUND_SHORT.",
                    "long_name": "Reporting Date",
                    "unit": "n/a"
                }
            },
            "UNREALISED_GROSS_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unrealised gross internal rate of return for the fund. Passed through unchanged from bronze.",
                    "long_name": "Unrealised Gross IRR",
                    "unit": "%"
                }
            },
            "UNREALISED_NET_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Unrealised net internal rate of return for the fund. Passed through unchanged from bronze.",
                    "long_name": "Unrealised Net IRR",
                    "unit": "%"
                }
            },
            "WEIGHTED_YIELD_UNITS": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Weighted units calculation used for yield analysis. Passed through unchanged from bronze.",
                    "long_name": "Weighted Yield Units",
                    "unit": "n/a"
                }
            },
            "ANNUAL_NET_IRR": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Annual net internal rate of return for the fund. Passed through unchanged from bronze.",
                    "long_name": "Annual Net IRR",
                    "unit": "%"
                }
            },
            "NAV_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Net asset value yield for the fund. Passed through unchanged from bronze.",
                    "long_name": "NAV Yield",
                    "unit": "%"
                }
            },
            "QUARTERLY_YIELD": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Quarterly yield for the fund. Passed through unchanged from bronze.",
                    "long_name": "Quarterly Yield",
                    "unit": "%"
                }
            },
            "datalake_ingestion_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "Timestamp of when the record was ingested into the Data Lake. Passed through unchanged from bronze.",
                    "long_name": "Data Lake Load Timestamp",
                    "unit": "n/a"
                }
            },
            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": "The system timestamp when this silver record was generated by the pipeline (CURRENT_TIMESTAMP() at query time), used to track load freshness rather than any source-system edit.",
                    "long_name": "Silver Refresh Timestamp",
                    "unit": "n/a"
                }
            }
        }
    },
}
