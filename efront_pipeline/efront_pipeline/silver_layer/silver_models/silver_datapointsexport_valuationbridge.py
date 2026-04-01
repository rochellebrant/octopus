# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_datapointsexport_valuationbridge_sql_code = '''
SELECT
  *,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM {bronze_prefix}datapointsexport
WHERE CATEGORY_NAME LIKE "Valuation Bridge%"
'''