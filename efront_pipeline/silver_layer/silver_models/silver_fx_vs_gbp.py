# Purpose
    # Build a normalized table of FX rates to GBP, one row per (Currency, Ref_Date), so downstream models
    # (e.g. silver_aum_capital_injections_and_disposals_transactions) can convert amounts in any supported
    # currency into GBP for a given reference date.

# Map raw currency names to ISO codes
    # The bronze fx table stores currencies by name (e.g. "Euro", "US Dollar"). curr_map translates the
    # supported currency names into their ISO codes (EUR, USD, AUD, SEK, PLN, NOK, JPY, KRW, CAD, GBP) and
    # fx_raw joins the bronze fx rows onto both the source and destination currency to get their codes.

# Normalize every rate to be "1 unit of Currency -> GBP"
    # The bronze fx table can store a pair either direction (e.g. GBP->EUR or EUR->GBP) with no guarantee
    # which one is present, so the direction has to be corrected before use:
        # fx_direct: rows already quoted as Currency -> GBP are used as-is (Rate_to_Gbp = Fx_Rate).
        # fx_inverse: rows quoted as GBP -> Currency are inverted (Rate_to_Gbp = 1 / Fx_Rate) so they also
        #             represent Currency -> GBP.
    # The two are unioned together so every non-GBP currency ends up with a single Rate_to_Gbp per Ref_Date,
    # regardless of which direction the source system happened to publish it in.
    # Both directions can be present in bronze for the same (Currency, Ref_Date), giving two slightly
    # different Rate_to_Gbp values (rounding/spread differs once a rate is inverted). Direct quotes are
    # kept over derived (inverted) ones since inverting introduces an extra rounding step.
silver_fx_vs_gbp_sql_code = """
WITH curr_map AS (
    SELECT * FROM VALUES
        ('GBP', 'British Pound'),
        ('EUR', 'Euro'),
        ('USD', 'US Dollar'),
        ('AUD', 'Australian Dollar'),
        ('SEK', 'Swedish Krona'),
        ('PLN', 'Polish Zloty'),
        ('NOK', 'Norwegian Kroner'),
        ('JPY', 'Japanese Yen'),
        ('KRW', 'South-Korean Won'),
        ('CAD', 'Canadian Dollar')
    AS t(curr_code, curr_name)
),
fx_raw AS (
    SELECT
        m_src.curr_code AS src_code,
        m_dest.curr_code AS dest_code,
        fx.Fx_Rate AS Fx_Rate,
        fx.Ref_Date AS Ref_Date
    FROM {bronze_prefix}fx fx
    INNER JOIN curr_map m_src ON fx.Source_Currency = m_src.curr_name
    INNER JOIN curr_map m_dest ON fx.Destination_Curr = m_dest.curr_name
),
fx_direct AS (
    SELECT
        src_code AS Currency,
        Ref_Date,
        Fx_Rate AS Rate_to_Gbp
    FROM fx_raw
    WHERE dest_code = 'GBP' AND src_code != 'GBP'
),
fx_inverse AS (
    SELECT
        dest_code AS currency,
        ref_date,
        1.0 / Fx_Rate AS rate_to_gbp
    FROM fx_raw
    WHERE src_code = 'GBP' AND dest_code != 'GBP'
),
fx_combined AS (
    SELECT currency, ref_date, rate_to_gbp, 1 AS is_direct FROM fx_direct
    UNION ALL
    SELECT currency, ref_date, rate_to_gbp, 0 AS is_direct FROM fx_inverse
)
SELECT
    currency,
    ref_date,
    rate_to_gbp,
    current_timestamp() AS refresh_timestamp
FROM fx_combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY currency, ref_date ORDER BY is_direct DESC) = 1
"""
