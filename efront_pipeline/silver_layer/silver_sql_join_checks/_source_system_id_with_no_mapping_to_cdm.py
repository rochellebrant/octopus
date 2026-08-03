source_system_id_check = """
SELECT
    DISTINCT
    source_system_id
FROM oegen_data_prod_prod.core_data_model.bronze_mapping_system_sources
WHERE END_AT IS NULL
"""