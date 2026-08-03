silver_fees_fund_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}fees_fund
"""
