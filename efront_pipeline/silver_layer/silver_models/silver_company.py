silver_company_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp FROM {bronze_prefix}company ef_c
LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_company map_c
    ON ef_c.Company_IQId = map_c.source_company_id
    WHERE map_c.source_system_id = 'SRCE_SYST_1001'
"""