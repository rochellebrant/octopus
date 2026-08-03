company_with_no_cdm_id_check = """
WITH deduped AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY m.company_core_id
            ORDER BY START_AT DESC
        ) AS rn
    FROM oegen_data_prod_prod.core_data_model.bronze_company_dim_core m
    WHERE m.END_AT IS NULL AND m.END_AT IS NULL 
)
SELECT *
FROM deduped
WHERE rn = 1
"""