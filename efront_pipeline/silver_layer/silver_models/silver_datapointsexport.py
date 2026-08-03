silver_datapointsexport_sql_code = """
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY CATEGORY_NAME, CREATIONDATE, DATAPOINT_NAME, ENTITY, REFDATE
        ORDER BY VALUENUM, VALUESTRING, datalake_ingestion_timestamp
    ) AS row_seq,
    current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}datapointsexport
"""
