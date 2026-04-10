fund_aum_metrics_check = """
WITH deduped AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY m.source_fund_id
            ORDER BY m.last_updated DESC
        ) AS rn
    FROM oegen_data_prod_prod.core_data_model.bronze_mapping_fund m
    WHERE m.source_system_id = 'SRCE_SYST_1001'
)
SELECT *
FROM deduped
WHERE rn = 1
"""