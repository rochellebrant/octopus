efront_portfolios_dim = '''
        SELECT DISTINCT
            Created_on,
            Modified_on,
            Modified_by,
            Portfolio AS EFRONT_PORTFOLIO_NAME,
            source_portfolio_name AS MAPPED_CDM_PORTFOLIO_NAME,
            source_portfolio_id AS MAPPED_CDM_PORTFOLIO_ID
        FROM {bronze_prefix}instruments i
        LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_mapping_portfolio m
            ON m.source_portfolio_name = i.Portfolio
        WHERE UPPER(m.source_system_id) = 'SRCE_SYST_1001'
'''