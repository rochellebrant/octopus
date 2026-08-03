gold_track_record_fund_sql_code = """
SELECT
  s.cdm_fund AS fund
  , s.inception_date
  , s.fund_is_active
  , COALESCE(s.metric, s.raw_metric) AS metric
  , COALESCE(s.definition, 'Undefined') AS definition
  , s.* EXCEPT (ef_fund_id, ef_fund, cdm_fund_id, cdm_fund, fund_currency, metric, raw_metric, definition, inception_date, fund_is_active, refresh_timestamp)
  , current_timestamp() AS refresh_timestamp
FROM {silver_prefix}track_record_fund s
"""