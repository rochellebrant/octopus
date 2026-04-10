gold_layer_definitions = {
    "fundoperations_investor_country_share_monthly": {
        "table": {
            "comment": "Gold layer reporting table providing a monthly time-series analysis of investor activity by country. Calculates the percentage share of commitments, capital calls, and distributions for each country relative to the total fund-level activity for both periodic and cumulative balances."
        },
        "columns": {
            # Dimensions
            "Fund": { "schema": {"data_type": "STRING"}, "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} },
            "Fund_Structure_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The legal or operational structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"} },
            "Fund_Status": { "schema": {"data_type": "STRING"}, "description": {"comment": "The current lifecycle status of the fund (e.g., Active, Fully Called).", "long_name": "Fund Status", "unit": "n/a"} },
            "Transaction_Currency": { "schema": {"data_type": "STRING"}, "description": {"comment": "The currency of the transactions (ISO code).", "long_name": "Currency", "unit": "n/a"} },
            "Investor_Country": { "schema": {"data_type": "STRING"}, "description": {"comment": "The country associated with the investor group.", "long_name": "Investor Country", "unit": "n/a"} },
            "Year_Month": { "schema": {"data_type": "STRING"}, "description": {"comment": "The reporting period in YYYY-MM format.", "long_name": "Reporting Period", "unit": "n/a"} },

            # Slice Absolutes (Country specific)
            "Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total new commitments made by this country during the month.", "long_name": "Monthly Commitments", "unit": "currency"} },
            "Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called from this country during the month.", "long_name": "Monthly Called", "unit": "currency"} },
            "Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this country during the month.", "long_name": "Monthly Distribution", "unit": "currency"} },
            "Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total commitments for this country since inception.", "long_name": "Cumulative Commitments", "unit": "currency"} },
            "Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called for this country since inception.", "long_name": "Cumulative Called", "unit": "currency"} },
            "Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this country since inception.", "long_name": "Cumulative Distribution", "unit": "currency"} },

            # Fund-Period Totals
            "Total_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments for the month across all countries.", "long_name": "Total Fund Commitments", "unit": "currency"} },
            "Total_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls for the month across all countries.", "long_name": "Total Fund Called", "unit": "currency"} },
            "Total_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions for the month across all countries.", "long_name": "Total Fund Distribution", "unit": "currency"} },
            "Total_Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments since inception.", "long_name": "Total Fund Cumulative Commitments", "unit": "currency"} },
            "Total_Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls since inception.", "long_name": "Total Fund Cumulative Called", "unit": "currency"} },
            "Total_Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions since inception.", "long_name": "Total Fund Cumulative Distribution", "unit": "currency"} },

            # Monthly % Shares
            "Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total fund commitments for the month.", "long_name": "Commitments % Share", "unit": "percent"} },
            "Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total fund capital calls for the month.", "long_name": "Called % Share", "unit": "percent"} },
            "Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total fund distributions for the month.", "long_name": "Distribution % Share", "unit": "percent"} },

            # Cumulative % Shares
            "Cumulative_Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total cumulative fund commitments.", "long_name": "Cumulative Commitments % Share", "unit": "percent"} },
            "Cumulative_Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total cumulative fund capital calls.", "long_name": "Cumulative Called % Share", "unit": "percent"} },
            "Cumulative_Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share of total cumulative fund distributions.", "long_name": "Cumulative Distribution % Share", "unit": "percent"} },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this country share breakdown was calculated.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "fundoperations_investor_country_share_quarterly": {
        "table": {
            "comment": "Gold layer reporting table providing a quarterly time-series analysis of investor activity by country. It calculates the percentage share of quarterly commitments, capital calls, and distributions for each country relative to the total fund-level quarterly activity, including cumulative inception-to-date shares."
        },
        "columns": {
            # Dimensions
            "Fund": { "schema": {"data_type": "STRING"}, "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} },
            "Fund_Structure_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The legal or operational structure of the fund (e.g., SICAV, LP).", "long_name": "Fund Structure Type", "unit": "n/a"} },
            "Fund_Status": { "schema": {"data_type": "STRING"}, "description": {"comment": "The current lifecycle status of the fund during that quarter.", "long_name": "Fund Status", "unit": "n/a"} },
            "Transaction_Currency": { "schema": {"data_type": "STRING"}, "description": {"comment": "The ISO currency code used for the transactions.", "long_name": "Currency", "unit": "n/a"} },
            "Investor_Country": { "schema": {"data_type": "STRING"}, "description": {"comment": "The country of domicile for the investor group.", "long_name": "Investor Country", "unit": "n/a"} },
            "Year_Quarter": { "schema": {"data_type": "STRING"}, "description": {"comment": "The reporting period in YYYY-QX format.", "long_name": "Reporting Quarter", "unit": "n/a"} },

            # Slice Absolutes (Country specific)
            "Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total new commitments made by investors in this country during the specific quarter.", "long_name": "Quarterly Commitments", "unit": "currency"} },
            "Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called from investors in this country during the specific quarter.", "long_name": "Quarterly Called", "unit": "currency"} },
            "Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to investors in this country during the specific quarter.", "long_name": "Quarterly Distribution", "unit": "currency"} },
            "Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total commitments for this country from inception up to the end of the quarter.", "long_name": "Cumulative Commitments", "unit": "currency"} },
            "Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called for this country from inception up to the end of the quarter.", "long_name": "Cumulative Called", "unit": "currency"} },
            "Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this country from inception up to the end of the quarter.", "long_name": "Cumulative Distribution", "unit": "currency"} },

            # Fund-Period Totals
            "Total_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments for the quarter across all investor countries.", "long_name": "Total Fund Quarterly Commitments", "unit": "currency"} },
            "Total_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls for the quarter across all investor countries.", "long_name": "Total Fund Quarterly Called", "unit": "currency"} },
            "Total_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions for the quarter across all investor countries.", "long_name": "Total Fund Quarterly Distribution", "unit": "currency"} },
            "Total_Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments since inception as of the quarter end.", "long_name": "Total Fund Cumulative Commitments", "unit": "currency"} },
            "Total_Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls since inception as of the quarter end.", "long_name": "Total Fund Cumulative Called", "unit": "currency"} },
            "Total_Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions since inception as of the quarter end.", "long_name": "Total Fund Cumulative Distribution", "unit": "currency"} },

            # Quarterly % Shares
            "Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total fund commitments for the quarter.", "long_name": "Quarterly Commitments % Share", "unit": "percent"} },
            "Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total fund capital calls for the quarter.", "long_name": "Quarterly Called % Share", "unit": "percent"} },
            "Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total fund distributions for the quarter.", "long_name": "Quarterly Distribution % Share", "unit": "percent"} },

            # Cumulative % Shares
            "Cumulative_Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total cumulative fund commitments to date.", "long_name": "Cumulative Commitments % Share", "unit": "percent"} },
            "Cumulative_Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total cumulative fund capital calls to date.", "long_name": "Cumulative Called % Share", "unit": "percent"} },
            "Cumulative_Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Country share percentage of the total cumulative fund distributions to date.", "long_name": "Cumulative Distribution % Share", "unit": "percent"} },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this quarterly share analysis was computed.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "fundoperations_investor_type_share_monthly": {
        "table": {
            "comment": "Gold layer reporting table providing a monthly time-series analysis of investor activity by investor type. It calculates the percentage share of monthly and cumulative commitments, capital calls, and distributions for each investor classification relative to the total fund-level activity."
        },
        "columns": {
            # Dimensions
            "Fund": { "schema": {"data_type": "STRING"}, "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} },
            "Fund_Structure_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The legal or operational structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"} },
            "Fund_Status": { "schema": {"data_type": "STRING"}, "description": {"comment": "The current lifecycle status of the fund during the reporting month.", "long_name": "Fund Status", "unit": "n/a"} },
            "Transaction_Currency": { "schema": {"data_type": "STRING"}, "description": {"comment": "The ISO currency code for the transactions.", "long_name": "Currency", "unit": "n/a"} },
            "Investor_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The classification of the investor (e.g., Pension Fund, Insurance Company, Family Office).", "long_name": "Investor Type", "unit": "n/a"} },
            "Year_Month": { "schema": {"data_type": "STRING"}, "description": {"comment": "The reporting period in YYYY-MM format.", "long_name": "Reporting Period", "unit": "n/a"} },

            # Slice Absolutes (Investor Type specific)
            "Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total new commitments made by this investor type during the month.", "long_name": "Monthly Commitments", "unit": "currency"} },
            "Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called from this investor type during the month.", "long_name": "Monthly Called", "unit": "currency"} },
            "Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this investor type during the month.", "long_name": "Monthly Distribution", "unit": "currency"} },
            "Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total commitments for this investor type since inception as of the month end.", "long_name": "Cumulative Commitments", "unit": "currency"} },
            "Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called for this investor type since inception as of the month end.", "long_name": "Cumulative Called", "unit": "currency"} },
            "Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this investor type since inception as of the month end.", "long_name": "Cumulative Distribution", "unit": "currency"} },

            # Fund-Period Totals
            "Total_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments for the month across all investor types.", "long_name": "Total Fund Monthly Commitments", "unit": "currency"} },
            "Total_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls for the month across all investor types.", "long_name": "Total Fund Monthly Called", "unit": "currency"} },
            "Total_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions for the month across all investor types.", "long_name": "Total Fund Monthly Distribution", "unit": "currency"} },
            "Total_Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments since inception.", "long_name": "Total Fund Cumulative Commitments", "unit": "currency"} },
            "Total_Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls since inception.", "long_name": "Total Fund Cumulative Called", "unit": "currency"} },
            "Total_Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions since inception.", "long_name": "Total Fund Cumulative Distribution", "unit": "currency"} },

            # Monthly % Shares
            "Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of the total fund commitments for the month.", "long_name": "Commitments % Share", "unit": "percent"} },
            "Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of the total fund capital calls for the month.", "long_name": "Called % Share", "unit": "percent"} },
            "Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of the total fund distributions for the month.", "long_name": "Distribution % Share", "unit": "percent"} },

            # Cumulative % Shares
            "Cumulative_Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund commitments.", "long_name": "Cumulative Commitments % Share", "unit": "percent"} },
            "Cumulative_Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund capital calls.", "long_name": "Cumulative Called % Share", "unit": "percent"} },
            "Cumulative_Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund distributions.", "long_name": "Cumulative Distribution % Share", "unit": "percent"} },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this investor type concentration model was computed.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "fundoperations_investor_type_share_quarterly": {
        "table": {
            "comment": "Gold layer reporting table providing a quarterly time-series analysis of investor activity by investor type. It calculates the percentage share of quarterly and cumulative commitments, capital calls, and distributions for each investor classification relative to total fund-level quarterly activity."
        },
        "columns": {
            # Dimensions
            "Fund": { "schema": {"data_type": "STRING"}, "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} },
            "Fund_Structure_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The legal or operational structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"} },
            "Fund_Status": { "schema": {"data_type": "STRING"}, "description": {"comment": "The current lifecycle status of the fund during the reporting quarter.", "long_name": "Fund Status", "unit": "n/a"} },
            "Transaction_Currency": { "schema": {"data_type": "STRING"}, "description": {"comment": "The ISO currency code for the transactions.", "long_name": "Currency", "unit": "n/a"} },
            "Investor_Type": { "schema": {"data_type": "STRING"}, "description": {"comment": "The classification of the investor (e.g., Institutional, Sovereign Wealth Fund).", "long_name": "Investor Type", "unit": "n/a"} },
            "Year_Quarter": { "schema": {"data_type": "STRING"}, "description": {"comment": "The reporting period in YYYY-QX format.", "long_name": "Reporting Quarter", "unit": "n/a"} },

            # Slice Absolutes (Investor Type specific)
            "Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total new commitments made by this investor type during the quarter.", "long_name": "Quarterly Commitments", "unit": "currency"} },
            "Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called from this investor type during the quarter.", "long_name": "Quarterly Called", "unit": "currency"} },
            "Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this investor type during the quarter.", "long_name": "Quarterly Distribution", "unit": "currency"} },
            "Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total commitments for this investor type since inception as of the quarter end.", "long_name": "Cumulative Commitments", "unit": "currency"} },
            "Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital called for this investor type since inception as of the quarter end.", "long_name": "Cumulative Called", "unit": "currency"} },
            "Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total capital distributed to this investor type since inception as of the quarter end.", "long_name": "Cumulative Distribution", "unit": "currency"} },

            # Fund-Period Totals
            "Total_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments for the quarter across all investor types.", "long_name": "Total Fund Quarterly Commitments", "unit": "currency"} },
            "Total_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls for the quarter across all investor types.", "long_name": "Total Fund Quarterly Called", "unit": "currency"} },
            "Total_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions for the quarter across all investor types.", "long_name": "Total Fund Quarterly Distribution", "unit": "currency"} },
            "Total_Cumulative_Commitments": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level commitments since inception as of the quarter end.", "long_name": "Total Fund Cumulative Commitments", "unit": "currency"} },
            "Total_Cumulative_Called": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level capital calls since inception as of the quarter end.", "long_name": "Total Fund Cumulative Called", "unit": "currency"} },
            "Total_Cumulative_Distribution": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "Total fund-level distributions since inception as of the quarter end.", "long_name": "Total Fund Cumulative Distribution", "unit": "currency"} },

            # Quarterly % Shares
            "Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total fund commitments for the quarter.", "long_name": "Quarterly Commitments % Share", "unit": "percent"} },
            "Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total fund capital calls for the quarter.", "long_name": "Quarterly Called % Share", "unit": "percent"} },
            "Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total fund distributions for the quarter.", "long_name": "Quarterly Distribution % Share", "unit": "percent"} },

            # Cumulative % Shares
            "Cumulative_Commitments_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund commitments.", "long_name": "Cumulative Commitments % Share", "unit": "percent"} },
            "Cumulative_Called_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund capital calls.", "long_name": "Cumulative Called % Share", "unit": "percent"} },
            "Cumulative_Distribution_Pct": { "schema": {"data_type": "DOUBLE"}, "description": {"comment": "The investor type's percentage share of total cumulative fund distributions.", "long_name": "Cumulative Distribution % Share", "unit": "percent"} },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this quarterly investor type analysis was computed.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "fundoperations_monthly_totals_by_fund": {
        "table": {
            "comment": "Gold layer reporting table providing total aggregated monthly and cumulative fund-level operational flows. This table serves as the denominator for monthly share calculations, tracking total commitments, capital calls, and distributions by fund and currency."
        },
        "columns": {
            # Dimensions
            "Fund": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} 
            },
            "Fund_Structure_Type": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The legal or operational structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"} 
            },
            "Transaction_Currency": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The ISO currency code for the transactions.", "long_name": "Currency", "unit": "n/a"} 
            },
            "Year_Month": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The reporting period in YYYY-MM format.", "long_name": "Reporting Period", "unit": "n/a"} 
            },

            # Periodic Totals
            "Total_Commitments": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total new commitments for the fund across all investors during the month.", "long_name": "Total Monthly Commitments", "unit": "currency"} 
            },
            "Total_Called": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total capital called for the fund across all investors during the month.", "long_name": "Total Monthly Called", "unit": "currency"} 
            },
            "Total_Distribution": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total capital distributed by the fund across all investors during the month.", "long_name": "Total Monthly Distribution", "unit": "currency"} 
            },

            # Cumulative Totals
            "Total_Cumulative_Commitments": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of commitments since fund inception up to the reporting month.", "long_name": "Total Cumulative Commitments", "unit": "currency"} 
            },
            "Total_Cumulative_Called": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of capital called since fund inception up to the reporting month.", "long_name": "Total Cumulative Called", "unit": "currency"} 
            },
            "Total_Cumulative_Distribution": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of distributions since fund inception up to the reporting month.", "long_name": "Total Cumulative Distribution", "unit": "currency"} 
            },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this monthly fund-level aggregation was last computed.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "fundoperations_quarterly_totals_by_fund": {
        "table": {
            "comment": "Gold layer reporting table providing quarterly aggregated and inception-to-date cumulative fund-level operational flows. It tracks total commitments, capital calls, and distributions by fund and currency, ordered chronologically by quarter."
        },
        "columns": {
            # Dimensions
            "Fund": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The name of the fund.", "long_name": "Fund Name", "unit": "n/a"} 
            },
            "Fund_Structure_Type": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The legal or operational structure of the fund.", "long_name": "Fund Structure Type", "unit": "n/a"} 
            },
            "Transaction_Currency": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The ISO currency code for the transactions.", "long_name": "Currency", "unit": "n/a"} 
            },
            "Year_Quarter": { 
                "schema": {"data_type": "STRING"}, 
                "description": {"comment": "The reporting period in YYYY-QX format (e.g., 2025-Q4).", "long_name": "Reporting Quarter", "unit": "n/a"} 
            },

            # Periodic Totals
            "Total_Commitments": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total new commitments for the fund across all investors during the specific quarter.", "long_name": "Total Quarterly Commitments", "unit": "currency"} 
            },
            "Total_Called": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total capital called for the fund across all investors during the specific quarter.", "long_name": "Total Quarterly Called", "unit": "currency"} 
            },
            "Total_Distribution": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "Total capital distributed by the fund across all investors during the specific quarter.", "long_name": "Total Quarterly Distribution", "unit": "currency"} 
            },

            # Cumulative Totals
            "Total_Cumulative_Commitments": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of fund commitments since inception up to the end of the reporting quarter.", "long_name": "Total Cumulative Commitments", "unit": "currency"} 
            },
            "Total_Cumulative_Called": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of fund capital calls since inception up to the end of the reporting quarter.", "long_name": "Total Cumulative Called", "unit": "currency"} 
            },
            "Total_Cumulative_Distribution": { 
                "schema": {"data_type": "DOUBLE"}, 
                "description": {"comment": "The running total of fund distributions since inception up to the end of the reporting quarter.", "long_name": "Total Cumulative Distribution", "unit": "currency"} 
            },

            "refresh_timestamp": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {"comment": "The system timestamp when this quarterly fund-level aggregation was calculated.", "long_name": "Refresh Timestamp", "unit": "n/a"}
            }
        }
    },

    "track_record_fund": {
        "table": {
            "comment": "Gold layer reporting table for fund-level performance metrics and track record history."
        },
        "columns": {
            "fund": {"schema": {"data_type": "STRING"}, "description": {"comment": "Standardised Fund Name.", "long_name": "Fund Name", "unit": "n/a"}},
            "inception_date": {"schema": {"data_type": "DATE"}, "description": {"comment": "Start date/vintage of fund.", "long_name": "Fund Inception Date", "unit": "n/a"}},
            "fund_is_active": {"schema": {"data_type": "BOOLEAN"}, "description": {"comment": "Active status flag.", "long_name": "Is Fund Active", "unit": "n/a"}},
            "metric": {"schema": {"data_type": "STRING"}, "description": {"comment": "Performance metric name.", "long_name": "Performance Metric Name", "unit": "n/a"}},
            "definition": {"schema": {"data_type": "STRING"}, "description": {"comment": "Calculation logic.", "long_name": "Metric Definition", "unit": "n/a"}},
            "refresh_timestamp": {"schema": {"data_type": "TIMESTAMP"}, "description": {"comment": "Refresh timestamp.", "long_name": "Refresh Timestamp", "unit": "n/a"}}
        }
    }
}