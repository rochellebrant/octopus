silver_fact_shareholdings_sql_code = """
WITH daily_transactions AS (
    SELECT
        company_id AS child_company_id,
        beneficial_holder AS parent_holder_name,
        TRY_CAST(date_of_transaction AS DATE) AS transaction_date,
        array_join(array_sort(collect_set(type_of_transaction)), ', ') AS transaction_type,
        SUM(
            CAST(amount_of_transaction AS DECIMAL(38,18)) * CAST(financial_weight_per_security AS DECIMAL(38,4))
        ) AS net_units,
        1.0 AS current_financial_weight,
        -- 1. Grab the max timestamp for this daily aggregated slice
        MAX(datalake_ingestion_timestamp) AS daily_ingest_ts
    FROM {bronze_prefix}api_shareholdings
    WHERE TRY_CAST(date_of_transaction AS DATE) <= CURRENT_DATE()
      AND `__END_AT` IS NULL
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
        last_value(dt.current_financial_weight, true) OVER (
            PARTITION BY g.child_company_id, g.parent_holder_name
            ORDER BY g.transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_financial_weight,
        -- 2. Carry the highest timestamp forward through the cumulative timeline
        MAX(dt.daily_ingest_ts) OVER (
            PARTITION BY g.child_company_id, g.parent_holder_name
            ORDER BY g.transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_ingest_ts
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
    -- 3. Converted DISTINCT to GROUP BY to pull the entity timestamp safely
    SELECT  
        internal_id, 
        company_id, 
        name AS company_name,
        MAX(datalake_ingestion_timestamp) AS entity_ingest_ts
    FROM {bronze_prefix}api_entity
    WHERE (archived = false OR archived is null)
      AND `__END_AT` IS NULL 
    GROUP BY 1, 2, 3
),
deduped_parent_lookup AS (
    SELECT company_name, parent_internal_id, parent_company_id, entity_ingest_ts
    FROM (
        SELECT 
            company_name, 
            internal_id AS parent_internal_id, 
            company_id AS parent_company_id,
            entity_ingest_ts,
            ROW_NUMBER() OVER(PARTITION BY company_name ORDER BY internal_id DESC) as rn
        FROM company_cdm_ids
    ) WHERE rn = 1
),
calculated_percentages AS (
    SELECT
        c.company_id AS child_company_id,
        c.internal_id AS internal_child_company_id,
        c2.parent_company_id,
        c2.parent_internal_id AS internal_parent_company_id,
        prt.transaction_type,
        prt.transaction_date,
        (100 * prt.units_held / NULLIF(crt.total_units_outstanding, 0)) * COALESCE(NULLIF(prt.current_financial_weight, 0.0), 1.0) AS ownership_percentage,
        -- 4. Get the latest timestamp between the transaction and the entity lookups
        GREATEST(
            prt.running_ingest_ts,
            COALESCE(c.entity_ingest_ts, prt.running_ingest_ts),
            COALESCE(c2.entity_ingest_ts, prt.running_ingest_ts)
        ) AS calc_ingest_ts
    FROM parent_running_totals prt
    INNER JOIN child_running_totals crt 
        ON prt.child_company_id = crt.child_company_id
        AND prt.transaction_date = crt.transaction_date
    INNER JOIN company_cdm_ids c 
        ON c.company_id = prt.child_company_id
    INNER JOIN deduped_parent_lookup c2 
        ON c2.company_name = prt.parent_holder_name
),
shareholdings_timeline AS (
    SELECT
        child_company_id,
        internal_child_company_id,
        parent_company_id,
        internal_parent_company_id,
        CAST(ownership_percentage AS FLOAT) AS ownership_percentage,
        COALESCE(transaction_type, 'Passive Dilution/Accretion') AS transaction_type, 
        TRY_CAST(transaction_date AS DATE) AS active_from_date,
        TRY_CAST(LEAD(transaction_date) OVER (
            PARTITION BY child_company_id, parent_company_id 
            ORDER BY transaction_date
        ) AS DATE) AS active_to_date,
        calc_ingest_ts
    FROM calculated_percentages
),
guarantees_timeline AS (
    SELECT
        g.company_id AS child_company_id,
        c.internal_id AS internal_child_company_id,
        c2.parent_company_id,
        c2.parent_internal_id AS internal_parent_company_id,
        CAST(g.percentage_held AS FLOAT) AS ownership_percentage,
        CONCAT('Partnership Appointment - ', g.membership_type) AS transaction_type,
        TRY_CAST(g.date_appointed AS DATE) AS active_from_date,
        TRY_CAST(g.date_resigned AS DATE) AS active_to_date,
        -- 5. Same logic for the unaggregated guarantees stream
        GREATEST(
            g.datalake_ingestion_timestamp,
            COALESCE(c.entity_ingest_ts, g.datalake_ingestion_timestamp),
            COALESCE(c2.entity_ingest_ts, g.datalake_ingestion_timestamp)
        ) AS calc_ingest_ts
    FROM {bronze_prefix}api_member_guarantees g
    INNER JOIN company_cdm_ids c ON c.company_id = g.company_id
    INNER JOIN deduped_parent_lookup c2 ON c2.company_name = g.member_name
    WHERE g.`__END_AT` IS NULL 
),
combined_timeline AS (
    SELECT * FROM shareholdings_timeline
    UNION ALL
    SELECT * FROM guarantees_timeline
)

SELECT 
    SHA2(CONCAT_WS('||', child_company_id, parent_company_id, CAST(active_from_date AS STRING)), 256) AS company_bridge_parent_id,
    child_company_id,
    internal_child_company_id,
    parent_company_id,
    internal_parent_company_id,
    ownership_percentage,
    transaction_type,
    active_from_date,
    active_to_date,
    -- 6. Output the proper sequence timestamp alongside the execution audit timestamp
    calc_ingest_ts AS source_ingestion_timestamp,
    current_timestamp() AS refresh_timestamp
FROM combined_timeline
-- 7. Removed `ownership_percentage > 0` bug so historical 0% closures route downstream
WHERE (active_to_date IS NULL OR active_from_date < active_to_date)
"""
