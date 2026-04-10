table_rules = {
    "aum_asset": {
        "future_REPORT_DATE": "REPORT_DATE > current_date()"
    },
    "aum_fund": {
        "future_REPORT_DATE": "REPORT_DATE > current_date()"
    },
    "company": {
        "future_Created_on": "Created_on > current_date()",
        "future_Modified_on": "Modified_on > current_date()"
    },
    "datapointsexport": {
        "future_CREATIONDATE": "CREATIONDATE > current_date()",
        "future_MODIFICATIONDATE": "MODIFICATIONDATE > current_date()",
        "future_REPORTINGDATE": "REPORTINGDATE > current_date()",
    },
    "fund": {
        "future_Created_on": "Created_on > current_date()",
        "future_Modified_on": "Modified_on > current_date()"
    },
    "fundoperations": {
        "future_Created_on": "Created_on > current_date() OR TRIM(`Created_on`) = ''",
        "future_Effective_date": "Effective_date > current_date() OR TRIM(`Effective_date`) = ''",
        "empty_Currency": "TRIM(`Currency`) = ''",
        "empty_Draft": "TRIM(`Draft`) = ''",
        "empty_Fund": "TRIM(`Fund`) = ''",
        "empty_Fund_IQId": "TRIM(`Fund_IQId`) = ''",
        "empty_Investor_Account": "TRIM(`Investor_Account`) = ''",
        "empty_Share": "TRIM(`Share`) = ''",
        "empty_Short_Name": "TRIM(`Short_Name`) = ''",
        "empty_Transaction_Investor_IQId": "TRIM(`Transaction_Investor_IQId`) = ''",
        "empty_Type": "TRIM(`Type`) = ''",
    },
    "instruments": {
        "future_Created_on": "Created_on > current_date()",
        "future_Modified_on": "Modified_on > current_date()",
    },
    "investoraccount": {
        "future_Created_on": "Created_on > current_date()",
        "future_Modified_on": "Modified_on > current_date()",
        "empty_Country": "TRIM(`Country`) = '' AND Investor NOT IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.{PREFIX}fund)",
        "empty_Currency": "TRIM(`Currency`) = ''",
        "empty_Fund": "TRIM(`Fund`) = '' AND Investor NOT IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.{PREFIX}fund)",
        "empty_Fund_IQId": "TRIM(`Fund_IQId`) = '' AND Investor NOT IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.{PREFIX}fund)",
        "empty_Investor": "TRIM(`Investor`) = ''",
        "empty_Investor_Account": "TRIM(`Investor_Account`) = '' AND Investor NOT IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.{PREFIX}fund)",
        "empty_Investor_Type": "TRIM(`Investor_Type`) = ''",
        "empty_Investor_Account_IQId": "TRIM(`Investor_Account_IQId`) = '' AND Investor NOT IN (SELECT Fund FROM oegen_data_prod_source.src_blackrock_efront_xio.{PREFIX}fund)",
    },
    "track_record_asset": {
        "negative_MOIC": "MOIC < 0",
    },
    "track_record_fund": {
        "negative_WEIGHTED_YIELD_UNITS": "WEIGHTED_YIELD_UNITS < 0",
    },
}
