silver_fund_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp FROM {bronze_prefix}fund ef_f
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_fund map_f
    ON ef_f.Fund_IQId = map_f.source_fund_id
    WHERE map_f.source_system_id = 'SRCE_SYST_1001'
"""