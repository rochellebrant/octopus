silver_fees_company_sql_code = """
SELECT *, current_timestamp() AS refresh_timestamp
FROM {bronze_prefix}fees_company
"""
