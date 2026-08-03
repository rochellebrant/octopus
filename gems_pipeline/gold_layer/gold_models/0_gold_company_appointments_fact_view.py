gold_company_appointments_fact_view_sql_code = """
SELECT * EXCEPT (__START_AT, __END_AT, is_deleted)
FROM {silver_prefix}company_appointments_fact a
WHERE
    a.`__END_AT` IS NULL
"""