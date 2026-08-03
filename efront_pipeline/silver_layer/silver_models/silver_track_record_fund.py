def _build_sql():
    # PIVOT requires a static column set; the notebook derives year columns dynamically
    # from whatever Dec-31 snapshots exist. 2000-2035 covers any plausible fund track
    # record; extend this range if a fund's history falls outside it.
    years = [str(y) for y in range(2000, 2036)]
    years_in_clause = ", ".join(f"'{y}'" for y in years)
    years_select = ", ".join(f"`{y}`" for y in years)

    return f"""
WITH ef_funds AS (
    SELECT Fund_IQid, Fund, Short_Name
    FROM (
        SELECT Fund_IQid, Fund, Short_Name,
               row_number() OVER (PARTITION BY Short_Name ORDER BY Fund_IQid) AS rn
        FROM {{bronze_prefix}}fund
    ) WHERE rn = 1
),
mapped_funds AS (
    SELECT source_fund_id, cdm_fund_id
    FROM (
        SELECT source_fund_id, cdm_fund_id,
               row_number() OVER (PARTITION BY source_fund_id ORDER BY source_fund_id) AS rn
        FROM oegen_data_prod_prod.core_data_model.bronze_mapping_fund
        WHERE source_system_id = 'SRCE_SYST_1001' AND END_AT IS NULL
    ) WHERE rn = 1
),
cdm_fund_core AS (
    SELECT fund_core_id, primary_fund_name_id, fund_start_date, fund_end_date, base_currency
    FROM (
        SELECT fund_core_id, primary_fund_name_id, fund_start_date, fund_end_date, base_currency,
               row_number() OVER (PARTITION BY fund_core_id ORDER BY fund_core_id) AS rn
        FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_core
        WHERE END_AT IS NULL
    ) WHERE rn = 1
),
cdm_fund_names AS (
    SELECT fund_name_id, fund_display_name
    FROM (
        SELECT fund_name_id, fund_display_name,
               row_number() OVER (PARTITION BY fund_name_id ORDER BY fund_name_id) AS rn
        FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_names
        WHERE END_AT IS NULL
    ) WHERE rn = 1
),
cdm_funds AS (
    SELECT
        c.fund_core_id,
        n.fund_display_name AS cdm_fund,
        c.base_currency AS fund_currency,
        c.fund_start_date,
        c.fund_end_date
    FROM cdm_fund_core c
    LEFT JOIN cdm_fund_names n ON c.primary_fund_name_id = n.fund_name_id
),
metric_dictionary AS (
    SELECT normalised_metric, definition
    FROM (
        SELECT normalised_metric, definition,
               row_number() OVER (PARTITION BY normalised_metric ORDER BY normalised_metric) AS rn
        FROM {{silver_prefix}}track_record_metric_dictionary
    ) WHERE rn = 1
),
metric_display_names AS (
    SELECT metric AS raw_metric, display_name
    FROM oegen_data_prod_prod.core_data_model.ref_metric_display_name_mapping
),
datapoints_norm AS (
    SELECT
        ENTITY AS ef_fund_id,
        REPORTINGDATE AS reporting_date,
        datalake_ingestion_timestamp,
        CASE DATAPOINT_NAME
            WHEN 'Total Shareholder Return' THEN 'total_shareholder_return'
            WHEN 'Total Shareholder Return (Annualised)' THEN 'total_shareholder_return_annualised'
            WHEN 'Total NAV Return' THEN 'total_nav_return'
            WHEN 'Total NAV Return (Annualised)' THEN 'total_nav_return_annualised'
            WHEN 'Annualised Dividend Paid' THEN 'annualised_dividend_paid'
            WHEN 'Weighted Average Discount Rate' THEN 'weighted_average_discount_rate'
        END AS raw_metric,
        VALUENUM
    FROM {{silver_prefix}}datapointsexport
    WHERE lower(CATEGORY_NAME) = 'fund metrics'
      AND lower(ENTITY_TYPE) = 'fund'
      AND DATAPOINT_NAME IN (
        'Total Shareholder Return', 'Total Shareholder Return (Annualised)',
        'Total NAV Return', 'Total NAV Return (Annualised)',
        'Annualised Dividend Paid', 'Weighted Average Discount Rate'
      )
),
dp_pivoted AS (
    SELECT
        ef_fund_id,
        reporting_date,
        datalake_ingestion_timestamp,
        max(CASE WHEN raw_metric = 'total_shareholder_return' THEN VALUENUM END) AS total_shareholder_return,
        max(CASE WHEN raw_metric = 'total_shareholder_return_annualised' THEN VALUENUM END) AS total_shareholder_return_annualised,
        max(CASE WHEN raw_metric = 'total_nav_return' THEN VALUENUM END) AS total_nav_return,
        max(CASE WHEN raw_metric = 'total_nav_return_annualised' THEN VALUENUM END) AS total_nav_return_annualised,
        max(CASE WHEN raw_metric = 'annualised_dividend_paid' THEN VALUENUM END) AS annualised_dividend_paid,
        max(CASE WHEN raw_metric = 'weighted_average_discount_rate' THEN VALUENUM END) AS weighted_average_discount_rate
    FROM datapoints_norm
    GROUP BY ef_fund_id, reporting_date, datalake_ingestion_timestamp
),
dp_mapped AS (
    SELECT
        dp.ef_fund_id,
        dp.reporting_date,
        dp.datalake_ingestion_timestamp AS dp_ingest_ts,
        dp.total_shareholder_return,
        dp.total_shareholder_return_annualised,
        dp.total_nav_return,
        dp.total_nav_return_annualised,
        dp.annualised_dividend_paid,
        dp.weighted_average_discount_rate,
        ef.Short_Name AS dp_ef_fund,
        mf.cdm_fund_id AS dp_cdm_fund_id,
        cf.cdm_fund AS dp_cdm_fund,
        cf.fund_currency AS dp_fund_currency,
        cf.fund_start_date AS dp_fund_start_date,
        cf.fund_end_date AS dp_fund_end_date
    FROM dp_pivoted dp
    LEFT JOIN ef_funds ef ON dp.ef_fund_id = ef.Fund_IQid
    LEFT JOIN mapped_funds mf ON dp.ef_fund_id = mf.source_fund_id
    LEFT JOIN cdm_funds cf ON mf.cdm_fund_id = cf.fund_core_id
),
tr_mapped AS (
    SELECT
        tr.REPORTING_DATE AS reporting_date,
        tr.ANNUALISED_YIELD,
        tr.ANNUAL_YIELD,
        tr.DPI,
        tr.MOIC,
        tr.NET_CAGR,
        tr.NET_TOTAL_RETURN,
        tr.UNREALISED_GROSS_IRR,
        tr.UNREALISED_NET_IRR,
        tr.WEIGHTED_YIELD_UNITS,
        tr.ANNUAL_NET_IRR,
        tr.NAV_YIELD,
        tr.QUARTERLY_YIELD,
        tr.FUND_SHORT AS ef_fund,
        tr.datalake_ingestion_timestamp AS tr_ingest_ts,
        mf.source_fund_id AS ef_fund_id,
        mf.cdm_fund_id,
        cf.cdm_fund,
        cf.fund_currency,
        cf.fund_start_date,
        cf.fund_end_date
    FROM {{silver_prefix}}track_record_fund_raw tr
    LEFT JOIN ef_funds ef ON tr.FUND_SHORT = ef.Short_Name
    LEFT JOIN mapped_funds mf ON ef.Fund_IQid = mf.source_fund_id
    LEFT JOIN cdm_funds cf ON mf.cdm_fund_id = cf.fund_core_id
),
joined AS (
    SELECT
        coalesce(tr.ef_fund_id, dp.ef_fund_id) AS ef_fund_id,
        coalesce(tr.reporting_date, dp.reporting_date) AS reporting_date,
        coalesce(tr.ef_fund, dp.dp_ef_fund) AS ef_fund,
        coalesce(tr.cdm_fund_id, dp.dp_cdm_fund_id) AS cdm_fund_id,
        coalesce(tr.cdm_fund, dp.dp_cdm_fund) AS cdm_fund,
        coalesce(tr.fund_currency, dp.dp_fund_currency) AS fund_currency,
        coalesce(tr.fund_start_date, dp.dp_fund_start_date) AS fund_start_date,
        coalesce(tr.fund_end_date, dp.dp_fund_end_date) AS fund_end_date,
        coalesce(tr.tr_ingest_ts, dp.dp_ingest_ts) AS datalake_ingestion_timestamp,
        tr.ANNUALISED_YIELD, tr.ANNUAL_YIELD, tr.DPI, tr.MOIC, tr.NET_CAGR, tr.NET_TOTAL_RETURN,
        tr.UNREALISED_GROSS_IRR, tr.UNREALISED_NET_IRR, tr.WEIGHTED_YIELD_UNITS, tr.ANNUAL_NET_IRR,
        tr.NAV_YIELD, tr.QUARTERLY_YIELD,
        dp.total_shareholder_return, dp.total_shareholder_return_annualised,
        dp.total_nav_return, dp.total_nav_return_annualised,
        dp.annualised_dividend_paid, dp.weighted_average_discount_rate
    FROM tr_mapped tr
    FULL OUTER JOIN dp_mapped dp
        ON tr.ef_fund_id <=> dp.ef_fund_id AND tr.reporting_date <=> dp.reporting_date
),
final_base AS (
    SELECT
        ef_fund_id,
        ef_fund,
        cdm_fund_id,
        cdm_fund,
        fund_currency,
        CASE
            WHEN to_date(reporting_date) >= fund_start_date
                 AND (to_date(reporting_date) < fund_end_date OR fund_end_date IS NULL)
            THEN true ELSE false
        END AS fund_is_active,
        to_date(fund_start_date) AS inception_date,
        to_date(reporting_date) AS report_date,
        ANNUAL_NET_IRR AS annual_net_irr,
        ANNUAL_YIELD AS annual_yield,
        annualised_dividend_paid,
        ANNUALISED_YIELD AS annualised_yield,
        DPI AS dpi,
        MOIC AS moic,
        NAV_YIELD AS nav_yield,
        NET_CAGR AS net_cagr,
        NET_TOTAL_RETURN AS net_total_return,
        QUARTERLY_YIELD AS quarterly_yield,
        total_nav_return,
        total_nav_return_annualised,
        total_shareholder_return,
        total_shareholder_return_annualised,
        UNREALISED_GROSS_IRR AS unrealised_gross_irr,
        UNREALISED_NET_IRR AS unrealised_net_irr,
        weighted_average_discount_rate,
        WEIGHTED_YIELD_UNITS AS weighted_yield_units,
        datalake_ingestion_timestamp
    FROM joined
),
fund_dimensions AS (
    SELECT ef_fund_id, ef_fund, cdm_fund_id, cdm_fund, fund_currency, inception_date, fund_is_active
    FROM (
        SELECT ef_fund_id, ef_fund, cdm_fund_id, cdm_fund, fund_currency, inception_date, fund_is_active,
               row_number() OVER (PARTITION BY ef_fund_id ORDER BY report_date DESC) AS rn
        FROM final_base
    ) WHERE rn = 1
),
melted AS (
    SELECT ef_fund_id, report_date, date_format(report_date, 'yyyy') AS report_date_str, raw_metric, value
    FROM final_base
    LATERAL VIEW stack(18,
        'annual_net_irr', annual_net_irr,
        'annual_yield', annual_yield,
        'annualised_dividend_paid', annualised_dividend_paid,
        'annualised_yield', annualised_yield,
        'dpi', dpi,
        'moic', moic,
        'nav_yield', nav_yield,
        'net_cagr', net_cagr,
        'net_total_return', net_total_return,
        'quarterly_yield', quarterly_yield,
        'total_nav_return', total_nav_return,
        'total_nav_return_annualised', total_nav_return_annualised,
        'total_shareholder_return', total_shareholder_return,
        'total_shareholder_return_annualised', total_shareholder_return_annualised,
        'unrealised_gross_irr', unrealised_gross_irr,
        'unrealised_net_irr', unrealised_net_irr,
        'weighted_average_discount_rate', weighted_average_discount_rate,
        'weighted_yield_units', weighted_yield_units
    ) melt_tbl AS raw_metric, value
),
latest AS (
    SELECT ef_fund_id, raw_metric, report_date AS latest_metric_date, value AS latest_metric_value
    FROM (
        SELECT ef_fund_id, raw_metric, report_date, value,
               row_number() OVER (PARTITION BY ef_fund_id, raw_metric ORDER BY report_date DESC) AS rn
        FROM melted
        WHERE value IS NOT NULL
    ) WHERE rn = 1
),
pivoted AS (
    SELECT * FROM (
        SELECT ef_fund_id, raw_metric, report_date_str, value
        FROM melted
        WHERE month(report_date) = 12 AND day(report_date) = 31
    )
    PIVOT (
        first(value) FOR report_date_str IN ({years_in_clause})
    )
),
tr_dpe AS (
    SELECT
        coalesce(p.ef_fund_id, l.ef_fund_id) AS ef_fund_id,
        coalesce(p.raw_metric, l.raw_metric) AS raw_metric,
        l.latest_metric_date,
        l.latest_metric_value,
        {years_select}
    FROM pivoted p
    FULL OUTER JOIN latest l
        ON p.ef_fund_id <=> l.ef_fund_id AND p.raw_metric <=> l.raw_metric
)
SELECT
    fd.ef_fund_id,
    fd.ef_fund,
    fd.cdm_fund_id,
    fd.cdm_fund,
    fd.fund_currency,
    fd.inception_date,
    fd.fund_is_active,
    t.raw_metric,
    mdn.display_name AS metric,
    md.definition,
    t.latest_metric_date,
    t.latest_metric_value,
    {years_select},
    current_timestamp() AS refresh_timestamp
FROM tr_dpe t
LEFT JOIN fund_dimensions fd ON t.ef_fund_id = fd.ef_fund_id
LEFT JOIN metric_display_names mdn ON t.raw_metric = mdn.raw_metric
LEFT JOIN metric_dictionary md ON t.raw_metric = md.normalised_metric
"""


silver_track_record_fund_sql_code = _build_sql()
