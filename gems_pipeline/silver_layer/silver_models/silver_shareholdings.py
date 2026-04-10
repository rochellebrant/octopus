silver_shareholdings_sql_code = """
WITH daily_transactions AS (
    SELECT
        company_id AS child_company_id,
        beneficial_holder AS parent_holder_name,
        -- Cast to DATE
        TRY_CAST(date_of_transaction AS DATE) AS transaction_date,
        array_join(array_sort(collect_set(type_of_transaction)), ', ') AS transaction_type,
        SUM(CAST(value_of_transaction AS DECIMAL(38,18))) AS net_units,
        MAX(CAST(financial_weight_per_security AS DECIMAL(38,4))) AS financial_weight
    FROM {bronze_prefix}api_shareholdings
    WHERE TRY_CAST(date_of_transaction AS DATE) <= CURRENT_DATE()
      AND __END_AT IS NULL
    GROUP BY 1, 2, 3
),
timeline_dates AS (
    SELECT DISTINCT child_company_id, transaction_date FROM daily_transactions
),
parents_per_child AS (
    SELECT DISTINCT child_company_id, parent_holder_name FROM daily_transactions
),
ownership_grid AS (
    SELECT p.child_company_id, p.parent_holder_name, t.transaction_date
    FROM parents_per_child p
    JOIN timeline_dates t ON p.child_company_id = t.child_company_id
),
parent_running_totals AS (
    SELECT
        g.child_company_id, 
        g.parent_holder_name, 
        g.transaction_date,
        dt.transaction_type, 
        SUM(COALESCE(dt.net_units, 0)) OVER (
            PARTITION BY g.child_company_id, g.parent_holder_name
            ORDER BY g.transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS units_held,
        last_value(dt.financial_weight, true) OVER (
            PARTITION BY g.child_company_id, g.parent_holder_name
            ORDER BY g.transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_financial_weight
    FROM ownership_grid g
    LEFT JOIN daily_transactions dt
        ON g.child_company_id = dt.child_company_id
        AND g.parent_holder_name = dt.parent_holder_name
        AND g.transaction_date = dt.transaction_date
),
child_running_totals AS (
    SELECT child_company_id, transaction_date, SUM(units_held) AS total_units_outstanding
    FROM parent_running_totals
    GROUP BY child_company_id, transaction_date
),
company_cdm_ids AS (
    SELECT DISTINCT internal_id, company_id, name AS company_name
    FROM {bronze_prefix}api_entity
    WHERE archived = false AND __END_AT IS NULL
),
deduped_parent_lookup AS (
    SELECT company_name, internal_id AS parent_internal_id
    FROM (
        SELECT company_name, internal_id,
            ROW_NUMBER() OVER(PARTITION BY company_name ORDER BY internal_id DESC) as rn
        FROM company_cdm_ids
    ) WHERE rn = 1
),
calculated_percentages AS (
    SELECT
        c.internal_id AS child_internal_id,
        c2.parent_internal_id,
        prt.transaction_type,
        prt.transaction_date,
        (100 * prt.units_held / NULLIF(crt.total_units_outstanding, 0)) * COALESCE(NULLIF(prt.current_financial_weight, 0.0), 1.0) AS ownership_percentage
    FROM parent_running_totals prt
    JOIN child_running_totals crt
        ON prt.child_company_id = crt.child_company_id
        AND prt.transaction_date = crt.transaction_date
    LEFT JOIN company_cdm_ids c ON c.company_id = prt.child_company_id
    LEFT JOIN deduped_parent_lookup c2 ON c2.company_name = prt.parent_holder_name
),
shareholdings_timeline AS (
    SELECT
        child_internal_id,
        parent_internal_id,
        CAST(ownership_percentage AS FLOAT) AS ownership_percentage,
        COALESCE(transaction_type, 'Passive Dilution/Accretion') AS transaction_type, 
        TRY_CAST(transaction_date AS DATE) AS active_from_date,
        TRY_CAST(LEAD(transaction_date) OVER (
            PARTITION BY child_internal_id, parent_internal_id 
            ORDER BY transaction_date
        ) AS DATE) AS active_to_date
    FROM calculated_percentages
    WHERE child_internal_id IS NOT NULL 
      AND parent_internal_id IS NOT NULL
),
guarantees_timeline AS (
    SELECT
        g.company_internal_id AS child_internal_id,
        c2.parent_internal_id,
        CAST(g.percentage_held AS FLOAT) AS ownership_percentage,
        CONCAT('Partnership Appointment - ', g.membership_type) AS transaction_type,
        TRY_CAST(g.date_appointed AS DATE) AS active_from_date,
        TRY_CAST(g.date_resigned AS DATE) AS active_to_date
    FROM {bronze_prefix}api_member_guarantees g
    LEFT JOIN deduped_parent_lookup c2 ON c2.company_name = g.member_name
    WHERE g.__END_AT IS NULL 
      AND g.company_internal_id IS NOT NULL
      AND c2.parent_internal_id IS NOT NULL
),
combined_timeline AS (
    SELECT * FROM shareholdings_timeline
    UNION ALL
    SELECT * FROM guarantees_timeline
)

SELECT 
    SHA2(CONCAT_WS('||', child_internal_id, parent_internal_id, CAST(active_from_date AS STRING)), 256) AS company_bridge_parent_id,
    child_internal_id AS company_core_id,
    parent_internal_id AS parent_company_id,
    ownership_percentage,
    transaction_type,
    active_from_date,
    active_to_date,
    current_timestamp() AS refresh_timestamp
FROM combined_timeline
WHERE ownership_percentage > 0 
  AND (active_to_date IS NULL OR active_from_date < active_to_date)
"""
