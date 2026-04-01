# Purpose 
    # Build a clean set of cashflow / transaction movements (subscriptions, advances, redemptions) at a consistent granularity, mapped to the business’s canonical Fund + Portfolio identifiers, and converted into GBP so it can be compared/combined with other GBP-based reporting (e.g., AUM/NAV).

# Selects only “real” transactions
    # Pulls transactions data and keeps non-drafts, has a Portfolio string value (empties are ignored), and only selected where type is:
        # Subscriptions / purchases (equity)
        # Advances / purchases (loan)
        # Sales / redemptions

# Attach “official” Fund and Portfolio identities
    # Uses internal reference/mapping tables to translate eFront names into:
        # CDM Fund ID (the canonical fund identifier used across the business)
        # CDM Portfolio ID (the canonical portfolio identifier)
    # This is essentially “make eFront naming consistent with our master data model”.

# Apply manual corrections for known naming exceptions
    # If eFront fund is ORIP or OEOW, it force-maps them to known correct fund IDs/names (“Sky”, “Vector”).
    # Otherwise it uses the normal fund mapping.

# Assign a reporting month-end date
    # Converts each transaction date into a month-end date (e.g., any date in Jan becomes 31 Jan).
    # This allows monthly reporting alignment.

# Convert transaction amounts into GBP
    # Takes each transaction in the fund’s base currency and converts it to GBP using the FX rate for that currency as of the month-end of the transaction date.
    # If no FX rate is found, it assumes 1.0 (i.e., “treat as already GBP” — which is a key assumption to sense-check).

silver_aum_metrics_enriched_sql_code = """
with transactions as (
  select
    Transaction_IQId AS transaction_id,
    Investment_Details AS ef_fund,
    Portfolio AS ef_portfolio,
    Company_Investor AS investor,
    Type AS type,
    Amount AS amount,
    Effective_date AS transaction_date
  from {bronze_prefix}transactions
  where lower(Draft) = "false"
    and trim(Portfolio) != ""
    and Type in (
      "EQ - Purchase/Subscription (w/o Commitment)",
      "EQ - Subscription (Capital Issue following Commitment)",
      "LN - Advance/Purchase (w/o commitment)",
      "LN - Advance (following Commitment)",
      "EQ - Sale/Redemption"
    )
)
, fund_details as (
  select * from oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc
  left join oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn on fdn.fund_name_id = fdc.primary_fund_name_id
  left join oegen_data_prod_prod.core_data_model.bronze_mapping_fund mfb on mfb.cdm_fund_id = fdc.fund_core_id
  where
    fdc.END_AT is null
    and fdn.END_AT is null
    and upper(mfb.source_system_id) = "SRCE_SYST_1001"
)
, portfolio_details as (
  select * from oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core ipdc
  left join oegen_data_prod_prod.core_data_model.bronze_mapping_portfolio mpb on mpb.cdm_portfolio_id = ipdc.investment_portfolio_id
  where
    ipdc.END_AT is null
    and upper(mpb.source_system_id) = "SRCE_SYST_1001"
)
, correct_funds as (
  select
    case
      when upper(trim(t.ef_fund)) = "ORIP" then "154DF9017A364F8091EBAEB00FFDE565" -- Sky
      when upper(trim(t.ef_fund)) = "OEOW" then "FFA63E0C568F4DBD9730865DEAA6CB67" -- Vector
      else coalesce(fund_details.source_fund_id) --ef_f.Fund_IQId, ef_fpp.ef_fund_id
    end as ef_fund_id,
    case
      when t.ef_fund = "ORIP" then "Sky" -- Sky
      when t.ef_fund = "OEOW" then "Vector" -- Vector
      else t.ef_fund
    end as ef_fund_corrected,
    t.*,
    last_day(t.transaction_date) as transaction_month_end
  from transactions t
  left join fund_details on fund_details.fund_display_name = t.ef_fund
)
, mapped_transactions as (
  select
    fund_details.cdm_fund_id,
    portfolio_details.cdm_portfolio_id,
    portfolio_details.source_portfolio_id as ef_portfolio_id,
    correct_funds.*,
    fund_details.base_currency as currency -- fund currency
  from correct_funds

  -- get fund through AUM
  -- left join ef_fund_portfolio_pairs ef_fpp on t.ef_portfolio = ef_fpp.ef_portfolio

  -- get fund through CDM Fund tables
  left join fund_details on fund_details.source_fund_id = correct_funds.ef_fund_id
  left join portfolio_details on portfolio_details.investment_portfolio_name = correct_funds.ef_portfolio
)

, ef_fx_rates as (
  select * from {silver_prefix}fx_vs_gbp
)
, mapped_and_converted_transactions as (
select
  g.*,
  ef.Ref_Date        as fx_ref_date,
  case
    when upper(trim(g.currency)) = 'GBP' then 1.0
    else ef.Rate_to_Gbp
  end as used_rate_to_gbp,

  case
    when upper(trim(g.currency)) = 'GBP' then g.Amount
    else g.Amount * ef.Rate_to_Gbp
  end as amount_gbp
from mapped_transactions g
left join ef_fx_rates ef
  on ef.Currency = g.currency
  and to_date(ef.Ref_Date) = last_day(to_date(g.transaction_date))
)
, transactions_with_matching_fund_portfolio_pairs_in_aum as (
select
  trans.transaction_id,
  trans.cdm_fund_id,
  trans.cdm_portfolio_id,
  trans.ef_fund_id,
  trans.ef_portfolio_id,
  ef_fund_corrected as ef_fund,
  trans.ef_portfolio,
  trans.investor,
  trans.type,
  trans.amount,
  trans.transaction_date,
  trans.transaction_month_end,
  trans.currency,
  trans.fx_ref_date,
  trans.used_rate_to_gbp,
  trans.amount_gbp
from mapped_and_converted_transactions trans
where exists (
    select 1
    from oegen_data_prod_prod.efront.silver_aum_metrics aum
    where aum.ef_fund_id = trans.ef_fund_id
      and aum.ef_portfolio_id = trans.ef_portfolio_id
  )
)

-- build report-date periods ONLY for fund+portfolio pairs that survived above CTE
, aum_report_dates_for_filtered_pairs as (
  select distinct
    aum.ef_fund_id,
    aum.ef_portfolio_id,
    aum.Report_Date as report_date
  from oegen_data_prod_prod.efront.silver_aum_metrics aum
  inner join (
    select distinct ef_fund_id, ef_portfolio_id
    from transactions_with_matching_fund_portfolio_pairs_in_aum
  ) fp
    on aum.ef_fund_id = fp.ef_fund_id
   and aum.ef_portfolio_id = fp.ef_portfolio_id
)

, aum_periods as (
  select
    ef_fund_id,
    ef_portfolio_id,
    report_date as period_end,
    lag(report_date) over (
      partition by ef_fund_id, ef_portfolio_id
      order by report_date
    ) as period_start,
    report_date
  from aum_report_dates_for_filtered_pairs
)

, transactions_with_bucket_status as (
  select
    t.*,
    p.report_date as tx_effective_report_date,
    case
      when p.report_date is null then 'unbucketed_tx'
      else 'bucketed_tx'
    end as tx_bucket_status,
    current_timestamp() as refresh_timestamp
  from transactions_with_matching_fund_portfolio_pairs_in_aum t
  left join aum_periods p
    on t.ef_fund_id = p.ef_fund_id
   and t.ef_portfolio_id = p.ef_portfolio_id
   and t.transaction_month_end <= p.period_end
   and (p.period_start is null or t.transaction_month_end > p.period_start)
)

select * from transactions_with_bucket_status
"""