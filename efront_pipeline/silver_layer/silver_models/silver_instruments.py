silver_instruments_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}instruments
"""
