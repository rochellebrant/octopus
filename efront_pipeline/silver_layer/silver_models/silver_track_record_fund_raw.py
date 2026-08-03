silver_track_record_fund_raw_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}track_record_fund
"""
