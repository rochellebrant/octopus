from bundle_params import get_bundle_param

_DATA_CATALOG = get_bundle_param("DATA_CATALOG", "xio.data_catalog", "oegen_data_prod_test")
_LANDING_SCHEMA = get_bundle_param("LANDING_SCHEMA", "xio.landing_schema", "efront_landing")
SOURCE_DATABASE = f"{_DATA_CATALOG}.{_LANDING_SCHEMA}"

AUM_METRICS_ENRICHED_SQL = """
WITH
-- 1. eFront <-> CDM id mapping tables (SRCE_SYST_1001 only)
fund_map AS (
    SELECT DISTINCT source_fund_id, cdm_fund_id
    FROM oegen_data_prod_prod.core_data_model.bronze_mapping_fund
    WHERE source_system_id = 'SRCE_SYST_1001'
),
portfolio_map AS (
    SELECT DISTINCT source_portfolio_id, cdm_portfolio_id
    FROM oegen_data_prod_prod.core_data_model.bronze_mapping_portfolio
    WHERE source_system_id = 'SRCE_SYST_1001'
),

-- 2. CDM fund / investment-portfolio display names
fund_names AS (
    SELECT DISTINCT
        fc.fund_core_id,
        fn.fund_display_name
    FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fc
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fn
        ON fc.primary_fund_name_id = fn.fund_name_id AND fn.END_AT IS NULL
    WHERE fc.END_AT IS NULL
),
invp_names AS (
    SELECT DISTINCT investment_portfolio_id, investment_portfolio_name
    FROM oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core
    WHERE END_AT IS NULL
),

-- 3. eFront AUM metrics with CDM fund/portfolio ids mapped on
mapped_efront AS (
    SELECT
        aum.*,
        fm.cdm_fund_id,
        pm.cdm_portfolio_id,
        fn.fund_display_name AS cdm_fund,
        ivn.investment_portfolio_name AS cdm_portfolio
    FROM {silver_prefix}aum_metrics aum
    LEFT JOIN fund_map fm ON aum.ef_fund_id = fm.source_fund_id
    LEFT JOIN portfolio_map pm ON aum.ef_portfolio = pm.source_portfolio_id
    LEFT JOIN fund_names fn ON fm.cdm_fund_id = fn.fund_core_id
    LEFT JOIN invp_names ivn ON pm.cdm_portfolio_id = ivn.investment_portfolio_id
),

-- 4. Match each distinct (cdm_fund_id, cdm_portfolio_id, REPORT_DATE) triple to the CDM
--    silver portfolio interval that was active on that date
cdm_overview_matched AS (
    SELECT DISTINCT
        me.cdm_fund_id,
        me.cdm_portfolio_id,
        me.REPORT_DATE,
        cdm.asset_names AS raw_asset_names,
        cdm.asset_types AS raw_asset_types,
        cdm.asset_lifecycle_phases AS raw_asset_lifecycle_phases,
        cdm.asset_ownership_phases AS raw_asset_ownership_phases,
        cdm.asset_countries AS raw_asset_countries,
        cdm.asset_technologies AS raw_asset_technologies,
        cdm.reporting_asset_names AS cdm_reporting_asset_names,
        cdm.reporting_asset_type AS cdm_reporting_asset_type,
        cdm.reporting_asset_lifecycle_phase AS cdm_reporting_asset_lifecycle_phase,
        cdm.reporting_asset_ownership_phase AS cdm_reporting_asset_ownership_phase,
        cdm.asset_details AS cdm_asset_details,
        cdm.asset_country_ids,
        cdm.asset_country_group_ids,
        cdm.asset_technology_name_ids
    FROM mapped_efront me
    INNER JOIN oegen_data_prod_prod.core_data_model_dev.silver_reporting_overview_investment_portfolio cdm
        ON cdm.fund_id = me.cdm_fund_id
       AND cdm.investment_portfolio_id = me.cdm_portfolio_id
       AND me.REPORT_DATE >= cdm.active_from_date
       AND (cdm.active_to_date IS NULL OR me.REPORT_DATE < cdm.active_to_date)
    WHERE me.cdm_fund_id IS NOT NULL AND me.cdm_portfolio_id IS NOT NULL
),

-- 5. Country grouping: active-only country ids -> control table -> distinct grouping names,
--    with a fallback to active-only country-group ids (passed through as raw CDM names)
--    when no active asset has a resolved country. Also resolve the raw active country
--    names (ungrouped) to support the INVP_1132 Portugal+Spain special case below.
country_ids_active_names AS (
    SELECT
        com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE,
        array_distinct(collect_list(gc.country_name)) AS active_country_names
    FROM (
        SELECT com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE, t.country_core_id
        FROM cdm_overview_matched com
        LATERAL VIEW OUTER explode(com.asset_country_ids) t AS country_core_id
    ) com_exploded
    LEFT JOIN oegen_data_prod_prod.core_data_model.gold_country gc
        ON gc.country_core_id = com_exploded.country_core_id
    GROUP BY com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE
),
country_ids_grouped AS (
    SELECT
        com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE,
        array_distinct(collect_list(cg.country_grouping_name)) AS grouped_country_names
    FROM (
        SELECT com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE, t.country_core_id
        FROM cdm_overview_matched com
        LATERAL VIEW OUTER explode(com.asset_country_ids) t AS country_core_id
    ) com_exploded
    LEFT JOIN {LANDING_DB}._ref_country_groupings cg
        ON cg.country_core_id = com_exploded.country_core_id
    GROUP BY com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE
),
country_group_ids_grouped AS (
    SELECT
        com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE,
        array_distinct(collect_list(bcg.country_group)) AS fallback_country_names
    FROM (
        SELECT com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE, t.country_groups_id
        FROM cdm_overview_matched com
        LATERAL VIEW OUTER explode(com.asset_country_group_ids) t AS country_groups_id
    ) com_exploded
    LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_country_dim_groups bcg
        ON bcg.country_groups_id = com_exploded.country_groups_id
    GROUP BY com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE
),
country_grouped AS (
    SELECT
        com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE,
        CASE
            WHEN com.cdm_portfolio_id = 'INVP_1132'
             AND array_contains(coalesce(cian.active_country_names, array()), 'Portugal')
             AND array_contains(coalesce(cian.active_country_names, array()), 'Spain')
            THEN 'Spain'
            WHEN size(coalesce(cig.grouped_country_names, array())) > 1 THEN 'Multi-Country'
            WHEN size(coalesce(cig.grouped_country_names, array())) = 1 THEN element_at(cig.grouped_country_names, 1)
            WHEN size(coalesce(cgg.fallback_country_names, array())) > 1 THEN 'Multi-Country'
            WHEN size(coalesce(cgg.fallback_country_names, array())) = 1 THEN element_at(cgg.fallback_country_names, 1)
            ELSE NULL
        END AS reporting_asset_country
    FROM cdm_overview_matched com
    LEFT JOIN country_ids_active_names cian
        ON com.cdm_fund_id = cian.cdm_fund_id AND com.cdm_portfolio_id = cian.cdm_portfolio_id AND com.REPORT_DATE = cian.REPORT_DATE
    LEFT JOIN country_ids_grouped cig
        ON com.cdm_fund_id = cig.cdm_fund_id AND com.cdm_portfolio_id = cig.cdm_portfolio_id AND com.REPORT_DATE = cig.REPORT_DATE
    LEFT JOIN country_group_ids_grouped cgg
        ON com.cdm_fund_id = cgg.cdm_fund_id AND com.cdm_portfolio_id = cgg.cdm_portfolio_id AND com.REPORT_DATE = cgg.REPORT_DATE
),

-- 6. Technology grouping: active-only technology ids -> control table -> distinct grouping
--    names, then apply the Battery-storage-combination / Multi-Tech-Renewables collapse
technology_ids_grouped AS (
    SELECT
        com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE,
        array_distinct(collect_list(tg.technology_grouping_name)) AS grouped_technology_names
    FROM (
        SELECT com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE, t.technology_name_id
        FROM cdm_overview_matched com
        LATERAL VIEW OUTER explode(com.asset_technology_name_ids) t AS technology_name_id
    ) com_exploded
    LEFT JOIN {LANDING_DB}._ref_tech_groupings tg
        ON tg.technology_name_id = com_exploded.technology_name_id
    GROUP BY com_exploded.cdm_fund_id, com_exploded.cdm_portfolio_id, com_exploded.REPORT_DATE
),
technology_grouped AS (
    SELECT
        com.cdm_fund_id, com.cdm_portfolio_id, com.REPORT_DATE,
        CASE
            WHEN size(coalesce(tig.grouped_technology_names, array())) = 2
                 AND array_contains(tig.grouped_technology_names, 'Battery storage')
            THEN concat(element_at(array_except(tig.grouped_technology_names, array('Battery storage')), 1), ' & Battery storage')
            WHEN size(coalesce(tig.grouped_technology_names, array())) >= 2 THEN 'Multi-Tech Renewables'
            WHEN size(coalesce(tig.grouped_technology_names, array())) = 1 THEN element_at(tig.grouped_technology_names, 1)
            ELSE NULL
        END AS reporting_asset_technology
    FROM cdm_overview_matched com
    LEFT JOIN technology_ids_grouped tig
        ON com.cdm_fund_id = tig.cdm_fund_id AND com.cdm_portfolio_id = tig.cdm_portfolio_id AND com.REPORT_DATE = tig.REPORT_DATE
),

-- 7. Combine the CDM overview with the resolved country/technology groupings, flattening
--    the raw (all incl. divested) arrays to ';'-joined strings and projecting asset_details
--    down from CDM's 12-field struct to the 6 fields required by the target schema
enriched AS (
    SELECT
        com.cdm_fund_id,
        com.cdm_portfolio_id,
        com.REPORT_DATE,
        array_join(com.raw_asset_names, '; ') AS asset_names,
        array_join(com.cdm_reporting_asset_names, '; ') AS reporting_asset_names,
        array_join(com.raw_asset_types, '; ') AS asset_types,
        com.cdm_reporting_asset_type AS reporting_asset_type,
        array_join(com.raw_asset_lifecycle_phases, '; ') AS asset_lifecycle_phases,
        com.cdm_reporting_asset_lifecycle_phase AS reporting_asset_lifecycle_phase,
        array_join(com.raw_asset_ownership_phases, '; ') AS asset_ownership_phases,
        com.cdm_reporting_asset_ownership_phase AS reporting_asset_ownership_phase,
        array_join(com.raw_asset_countries, '; ') AS asset_countries,
        cg.reporting_asset_country,
        array_join(com.raw_asset_technologies, '; ') AS asset_technologies,
        tg.reporting_asset_technology,
        transform(
            com.cdm_asset_details,
            x -> named_struct(
                'name', x.name,
                'type', x.type,
                'country', x.country,
                'technology', x.technology,
                'lifecycle_phase', x.lifecycle_phase,
                'ownership_phase', x.ownership_phase
            )
        ) AS asset_details
    FROM cdm_overview_matched com
    LEFT JOIN country_grouped cg
        ON com.cdm_fund_id = cg.cdm_fund_id AND com.cdm_portfolio_id = cg.cdm_portfolio_id AND com.REPORT_DATE = cg.REPORT_DATE
    LEFT JOIN technology_grouped tg
        ON com.cdm_fund_id = tg.cdm_fund_id AND com.cdm_portfolio_id = tg.cdm_portfolio_id AND com.REPORT_DATE = tg.REPORT_DATE
),

-- 8. Left join the enrichment onto every eFront row (unmatched rows keep NULL enrichment)
joined AS (
    SELECT
        me.*,
        e.asset_names, e.reporting_asset_names, e.asset_types, e.reporting_asset_type,
        e.asset_lifecycle_phases, e.reporting_asset_lifecycle_phase,
        e.asset_ownership_phases, e.reporting_asset_ownership_phase,
        e.asset_countries, e.reporting_asset_country,
        e.asset_technologies, e.reporting_asset_technology,
        e.asset_details
    FROM mapped_efront me
    LEFT JOIN enriched e
        ON me.cdm_fund_id = e.cdm_fund_id
       AND me.cdm_portfolio_id = e.cdm_portfolio_id
       AND me.REPORT_DATE = e.REPORT_DATE
),

-- 9. Apply Undeployed / N/A overrides to the asset-derived columns
overridden AS (
    SELECT
        j.* EXCEPT (asset_names, reporting_asset_names, asset_types, reporting_asset_type, asset_lifecycle_phases, reporting_asset_lifecycle_phase, asset_ownership_phases, reporting_asset_ownership_phase, asset_countries, reporting_asset_country, asset_technologies, reporting_asset_technology, asset_details),
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_names END AS asset_names,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_names END AS reporting_asset_names,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_types END AS asset_types,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_type END AS reporting_asset_type,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_lifecycle_phases END AS asset_lifecycle_phases,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_lifecycle_phase END AS reporting_asset_lifecycle_phase,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_ownership_phases END AS asset_ownership_phases,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_ownership_phase END AS reporting_asset_ownership_phase,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_countries END AS asset_countries,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_country END AS reporting_asset_country,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.asset_technologies END AS asset_technologies,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN 'N/A' ELSE j.reporting_asset_technology END AS reporting_asset_technology,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN ARRAY(named_struct('name', 'Undeployed', 'type', 'Undeployed', 'country', 'Undeployed', 'technology', 'Undeployed', 'lifecycle_phase', 'Undeployed', 'ownership_phase', 'Undeployed')) WHEN ((cdm_fund_id = 'FUND_1017' AND cdm_portfolio_id IN ('INVP_1160', 'INVP_1161', 'INVP_1162', 'n/a_Sustainable_Growth_Fund_Equities', 'n/a_Sustainable_Growth_Fund_Investment_Trusts', 'n/a_Undeployed')) OR cdm_portfolio_id LIKE 'n/a_%' OR cdm_portfolio_id = 'INVP_1007') THEN ARRAY(named_struct('name', 'N/A', 'type', 'N/A', 'country', 'N/A', 'technology', 'N/A', 'lifecycle_phase', 'N/A', 'ownership_phase', 'N/A')) ELSE j.asset_details END AS asset_details
    FROM joined j
),

-- 10. SGF Fund-of-Funds elimination: UNION ALL in negated duplicate rows for the 3 SGF
--     sub-portfolios under FUND_1017, to avoid double-counting AUM. Both branches use the
--     identical `* EXCEPT(...)` + re-added-columns-in-the-same-order shape so the UNION ALL
--     lines up positionally regardless of UNION ALL BY NAME support.
eliminated AS (
    SELECT
        * EXCEPT (
        ef_fund_id,
        ef_portfolio_id,
        cdm_fund_id,
        cdm_portfolio_id,
        ef_fund,
        ef_portfolio,
        cdm_fund,
        cdm_portfolio,
        FUND_CASH_UP,
        FUND_CASH_DOWN,
        FUND_CASH_EXP,
        FUND_CAPITAL_DR,
        FUND_CASH_OTHER,
        FUND_VALUE_ADJ,
        FUND_REMOVE_DBL,
        FUND_CONDITIONAL_ACQ_EQ,
        TOTAL_FUND_DEBT,
        UNDRAWN_FUND_DEBT,
        TOTAL_COMMITTED,
        EXPIRED_COMMITMENT,
        TOTAL_CALLED,
        RETURN_OF_CALL,
        TOTAL_RECALLED,
        TOTAL_FUND_CAPITAL_HR,
        EQ_VALUATION,
        LN_VALUATION,
        LOAN_BALANCE,
        ASSET_DEBT,
        ASSET_DEBT_CMT,
        ASSET_EQUITY_CMT,
        EXTERNAL_DEBT_COMMIT,
        ADJ_UNDRAWNCOMMITMENT,
        ASSET_LEVEL_CASH,
        ASSET_LEVEL_CASH_ADJ,
        ASSET_NAV,
        FUND_NAV,
        FUM,
        PORTFOLIO_GAV,
        FUND_GAV,
        AUM,
        DRY_POWDER,
        CHECK_ASSET_NAV,
        CHECK_FUND_NAV,
        CHECK_FUM,
        CHECK_PORTFOLIO_GAV,
        CHECK_FUND_GAV,
        CHECK_AUM,
        CHECK_DRY_POWDER
        ),
        ef_fund_id,
        ef_portfolio_id,
        cdm_fund_id,
        cdm_portfolio_id,
        ef_fund,
        ef_portfolio,
        cdm_fund,
        cdm_portfolio,
        FUND_CASH_UP,
        FUND_CASH_DOWN,
        FUND_CASH_EXP,
        FUND_CAPITAL_DR,
        FUND_CASH_OTHER,
        FUND_VALUE_ADJ,
        FUND_REMOVE_DBL,
        FUND_CONDITIONAL_ACQ_EQ,
        TOTAL_FUND_DEBT,
        UNDRAWN_FUND_DEBT,
        TOTAL_COMMITTED,
        EXPIRED_COMMITMENT,
        TOTAL_CALLED,
        RETURN_OF_CALL,
        TOTAL_RECALLED,
        TOTAL_FUND_CAPITAL_HR,
        EQ_VALUATION,
        LN_VALUATION,
        LOAN_BALANCE,
        ASSET_DEBT,
        ASSET_DEBT_CMT,
        ASSET_EQUITY_CMT,
        EXTERNAL_DEBT_COMMIT,
        ADJ_UNDRAWNCOMMITMENT,
        ASSET_LEVEL_CASH,
        ASSET_LEVEL_CASH_ADJ,
        ASSET_NAV,
        FUND_NAV,
        FUM,
        PORTFOLIO_GAV,
        FUND_GAV,
        AUM,
        DRY_POWDER,
        CHECK_ASSET_NAV,
        CHECK_FUND_NAV,
        CHECK_FUM,
        CHECK_PORTFOLIO_GAV,
        CHECK_FUND_GAV,
        CHECK_AUM,
        CHECK_DRY_POWDER
    FROM overridden

    UNION ALL

    SELECT
        * EXCEPT (
        ef_fund_id,
        ef_portfolio_id,
        cdm_fund_id,
        cdm_portfolio_id,
        ef_fund,
        ef_portfolio,
        cdm_fund,
        cdm_portfolio,
        FUND_CASH_UP,
        FUND_CASH_DOWN,
        FUND_CASH_EXP,
        FUND_CAPITAL_DR,
        FUND_CASH_OTHER,
        FUND_VALUE_ADJ,
        FUND_REMOVE_DBL,
        FUND_CONDITIONAL_ACQ_EQ,
        TOTAL_FUND_DEBT,
        UNDRAWN_FUND_DEBT,
        TOTAL_COMMITTED,
        EXPIRED_COMMITMENT,
        TOTAL_CALLED,
        RETURN_OF_CALL,
        TOTAL_RECALLED,
        TOTAL_FUND_CAPITAL_HR,
        EQ_VALUATION,
        LN_VALUATION,
        LOAN_BALANCE,
        ASSET_DEBT,
        ASSET_DEBT_CMT,
        ASSET_EQUITY_CMT,
        EXTERNAL_DEBT_COMMIT,
        ADJ_UNDRAWNCOMMITMENT,
        ASSET_LEVEL_CASH,
        ASSET_LEVEL_CASH_ADJ,
        ASSET_NAV,
        FUND_NAV,
        FUM,
        PORTFOLIO_GAV,
        FUND_GAV,
        AUM,
        DRY_POWDER,
        CHECK_ASSET_NAV,
        CHECK_FUND_NAV,
        CHECK_FUM,
        CHECK_PORTFOLIO_GAV,
        CHECK_FUND_GAV,
        CHECK_AUM,
        CHECK_DRY_POWDER
        ),
        'n/a_elimination_bodge_sgf' AS ef_fund_id,
        CASE
            WHEN ef_portfolio LIKE 'SGF (Vector%' THEN 'SGF (Vector) [ELIMINATION]'
            WHEN ef_portfolio LIKE 'SGF (Sky%' THEN 'SGF (Sky) [ELIMINATION]'
            WHEN ef_portfolio LIKE 'SGF (OETF%' THEN 'SGF (OETF) [ELIMINATION]'
        END AS ef_portfolio_id,
        'n/a_elimination_bodge_sgf' AS cdm_fund_id,
        CASE
            WHEN ef_portfolio LIKE 'SGF (Vector%' THEN 'n/a_elimination_bodge_sgf_vector'
            WHEN ef_portfolio LIKE 'SGF (Sky%' THEN 'n/a_elimination_bodge_sgf_sky'
            WHEN ef_portfolio LIKE 'SGF (OETF%' THEN 'n/a_elimination_bodge_sgf_oetf'
        END AS cdm_portfolio_id,
        concat(ef_fund, ' [ELIMINATION]') AS ef_fund,
        concat(ef_portfolio, ' [ELIMINATION]') AS ef_portfolio,
        concat(cdm_fund, ' [ELIMINATION]') AS cdm_fund,
        concat(cdm_portfolio, ' [ELIMINATION]') AS cdm_portfolio,
        -FUND_CASH_UP AS FUND_CASH_UP,
        -FUND_CASH_DOWN AS FUND_CASH_DOWN,
        -FUND_CASH_EXP AS FUND_CASH_EXP,
        -FUND_CAPITAL_DR AS FUND_CAPITAL_DR,
        -FUND_CASH_OTHER AS FUND_CASH_OTHER,
        -FUND_VALUE_ADJ AS FUND_VALUE_ADJ,
        -FUND_REMOVE_DBL AS FUND_REMOVE_DBL,
        -FUND_CONDITIONAL_ACQ_EQ AS FUND_CONDITIONAL_ACQ_EQ,
        -TOTAL_FUND_DEBT AS TOTAL_FUND_DEBT,
        -UNDRAWN_FUND_DEBT AS UNDRAWN_FUND_DEBT,
        -TOTAL_COMMITTED AS TOTAL_COMMITTED,
        -EXPIRED_COMMITMENT AS EXPIRED_COMMITMENT,
        -TOTAL_CALLED AS TOTAL_CALLED,
        -RETURN_OF_CALL AS RETURN_OF_CALL,
        -TOTAL_RECALLED AS TOTAL_RECALLED,
        -TOTAL_FUND_CAPITAL_HR AS TOTAL_FUND_CAPITAL_HR,
        -EQ_VALUATION AS EQ_VALUATION,
        -LN_VALUATION AS LN_VALUATION,
        -LOAN_BALANCE AS LOAN_BALANCE,
        -ASSET_DEBT AS ASSET_DEBT,
        -ASSET_DEBT_CMT AS ASSET_DEBT_CMT,
        -ASSET_EQUITY_CMT AS ASSET_EQUITY_CMT,
        -EXTERNAL_DEBT_COMMIT AS EXTERNAL_DEBT_COMMIT,
        -ADJ_UNDRAWNCOMMITMENT AS ADJ_UNDRAWNCOMMITMENT,
        -ASSET_LEVEL_CASH AS ASSET_LEVEL_CASH,
        -ASSET_LEVEL_CASH_ADJ AS ASSET_LEVEL_CASH_ADJ,
        -ASSET_NAV AS ASSET_NAV,
        -FUND_NAV AS FUND_NAV,
        -FUM AS FUM,
        -PORTFOLIO_GAV AS PORTFOLIO_GAV,
        -FUND_GAV AS FUND_GAV,
        -AUM AS AUM,
        -DRY_POWDER AS DRY_POWDER,
        -CHECK_ASSET_NAV AS CHECK_ASSET_NAV,
        -CHECK_FUND_NAV AS CHECK_FUND_NAV,
        -CHECK_FUM AS CHECK_FUM,
        -CHECK_PORTFOLIO_GAV AS CHECK_PORTFOLIO_GAV,
        -CHECK_FUND_GAV AS CHECK_FUND_GAV,
        -CHECK_AUM AS CHECK_AUM,
        -CHECK_DRY_POWDER AS CHECK_DRY_POWDER
    FROM overridden
    WHERE cdm_fund_id = 'FUND_1017'
      AND (ef_portfolio LIKE 'SGF (Vector%' OR ef_portfolio LIKE 'SGF (Sky%' OR ef_portfolio LIKE 'SGF (OETF%')
),

-- 11. Fresh aggregate pipeline-type / investment-strategy / fund-structure lookups (computed
--     across ALL CDM rows, independent of any version-scoping in the silver portfolio table)
invp_details_agg AS (
    SELECT
        investment_portfolio_id,
        NULLIF(concat_ws(', ', sort_array(collect_set(inv_pipeline_type))), '') AS inv_pipeline_type,
        NULLIF(concat_ws(', ', sort_array(collect_set(invp_investment_strategy))), '') AS invp_investment_strategy
    FROM oegen_data_prod_prod.core_data_model.bronze_investment_portfolio_dim_core
    WHERE END_AT IS NULL
    GROUP BY investment_portfolio_id
),
fund_structs_agg AS (
    SELECT
        fc.fund_core_id,
        NULLIF(concat_ws(', ', sort_array(collect_set(fst.fund_structure_type_name))), '') AS fund_structure_type_name
    FROM oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fc
    INNER JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_structure_type fst
        ON fc.fund_structure_type_id = fst.fund_structure_type_id
    WHERE fc.END_AT IS NULL
    GROUP BY fc.fund_core_id
),
with_agg AS (
    SELECT
        el.*,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' ELSE ida.inv_pipeline_type END AS inv_pipeline_type,
        CASE WHEN lower(ef_portfolio) = 'undeployed' THEN 'Undeployed' ELSE ida.invp_investment_strategy END AS invp_investment_strategy,
        fsa.fund_structure_type_name
    FROM eliminated el
    LEFT JOIN invp_details_agg ida ON el.cdm_portfolio_id = ida.investment_portfolio_id
    LEFT JOIN fund_structs_agg fsa ON el.cdm_fund_id = fsa.fund_core_id
),

-- 12. N/A cascade: once cdm_portfolio_id / cdm_fund_id has been rewritten to an 'n/a_...' bodge
--     value (either originally or by the elimination step above), null out the corresponding
--     aggregate columns too -- unless it's the 'Undeployed' portfolio/fund
na_overridden AS (
    SELECT
        wa.* EXCEPT (inv_pipeline_type, invp_investment_strategy, fund_structure_type_name),
        CASE WHEN wa.cdm_portfolio_id RLIKE '(?i)^n/a' AND wa.cdm_portfolio_id NOT RLIKE '(?i)^Undeployed' THEN 'N/A' ELSE wa.inv_pipeline_type END AS inv_pipeline_type,
        CASE WHEN wa.cdm_portfolio_id RLIKE '(?i)^n/a' AND wa.cdm_portfolio_id NOT RLIKE '(?i)^Undeployed' THEN 'N/A' ELSE wa.invp_investment_strategy END AS invp_investment_strategy,
        CASE WHEN wa.cdm_fund_id RLIKE '(?i)^n/a' AND wa.cdm_fund_id NOT RLIKE '(?i)^Undeployed' THEN 'N/A' ELSE wa.fund_structure_type_name END AS fund_structure_type_name
    FROM with_agg wa
),

-- 13. Forward-fill each OVERRIDE_COLS column (+ asset_details) within (ef_fund_id, ef_portfolio_id)
--     ordered by REPORT_DATE, and track whether any column on the row was backfilled
ff_computed AS (
    SELECT
        na.*,
        CASE WHEN asset_names = '' OR asset_names IS NULL THEN NULL ELSE asset_names END AS _clean_asset_names,
        LAST_VALUE(CASE WHEN asset_names = '' OR asset_names IS NULL THEN NULL ELSE asset_names END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_names,
        CASE WHEN reporting_asset_names = '' OR reporting_asset_names IS NULL THEN NULL ELSE reporting_asset_names END AS _clean_reporting_asset_names,
        LAST_VALUE(CASE WHEN reporting_asset_names = '' OR reporting_asset_names IS NULL THEN NULL ELSE reporting_asset_names END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_names,
        CASE WHEN asset_types = '' OR asset_types IS NULL THEN NULL ELSE asset_types END AS _clean_asset_types,
        LAST_VALUE(CASE WHEN asset_types = '' OR asset_types IS NULL THEN NULL ELSE asset_types END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_types,
        CASE WHEN reporting_asset_type = '' OR reporting_asset_type IS NULL THEN NULL ELSE reporting_asset_type END AS _clean_reporting_asset_type,
        LAST_VALUE(CASE WHEN reporting_asset_type = '' OR reporting_asset_type IS NULL THEN NULL ELSE reporting_asset_type END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_type,
        CASE WHEN asset_lifecycle_phases = '' OR asset_lifecycle_phases IS NULL THEN NULL ELSE asset_lifecycle_phases END AS _clean_asset_lifecycle_phases,
        LAST_VALUE(CASE WHEN asset_lifecycle_phases = '' OR asset_lifecycle_phases IS NULL THEN NULL ELSE asset_lifecycle_phases END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_lifecycle_phases,
        CASE WHEN reporting_asset_lifecycle_phase = '' OR reporting_asset_lifecycle_phase IS NULL THEN NULL ELSE reporting_asset_lifecycle_phase END AS _clean_reporting_asset_lifecycle_phase,
        LAST_VALUE(CASE WHEN reporting_asset_lifecycle_phase = '' OR reporting_asset_lifecycle_phase IS NULL THEN NULL ELSE reporting_asset_lifecycle_phase END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_lifecycle_phase,
        CASE WHEN asset_ownership_phases = '' OR asset_ownership_phases IS NULL THEN NULL ELSE asset_ownership_phases END AS _clean_asset_ownership_phases,
        LAST_VALUE(CASE WHEN asset_ownership_phases = '' OR asset_ownership_phases IS NULL THEN NULL ELSE asset_ownership_phases END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_ownership_phases,
        CASE WHEN reporting_asset_ownership_phase = '' OR reporting_asset_ownership_phase IS NULL THEN NULL ELSE reporting_asset_ownership_phase END AS _clean_reporting_asset_ownership_phase,
        LAST_VALUE(CASE WHEN reporting_asset_ownership_phase = '' OR reporting_asset_ownership_phase IS NULL THEN NULL ELSE reporting_asset_ownership_phase END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_ownership_phase,
        CASE WHEN asset_countries = '' OR asset_countries IS NULL THEN NULL ELSE asset_countries END AS _clean_asset_countries,
        LAST_VALUE(CASE WHEN asset_countries = '' OR asset_countries IS NULL THEN NULL ELSE asset_countries END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_countries,
        CASE WHEN reporting_asset_country = '' OR reporting_asset_country IS NULL THEN NULL ELSE reporting_asset_country END AS _clean_reporting_asset_country,
        LAST_VALUE(CASE WHEN reporting_asset_country = '' OR reporting_asset_country IS NULL THEN NULL ELSE reporting_asset_country END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_country,
        CASE WHEN asset_technologies = '' OR asset_technologies IS NULL THEN NULL ELSE asset_technologies END AS _clean_asset_technologies,
        LAST_VALUE(CASE WHEN asset_technologies = '' OR asset_technologies IS NULL THEN NULL ELSE asset_technologies END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_asset_technologies,
        CASE WHEN reporting_asset_technology = '' OR reporting_asset_technology IS NULL THEN NULL ELSE reporting_asset_technology END AS _clean_reporting_asset_technology,
        LAST_VALUE(CASE WHEN reporting_asset_technology = '' OR reporting_asset_technology IS NULL THEN NULL ELSE reporting_asset_technology END, true) OVER (PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _filled_reporting_asset_technology,
        LAST_VALUE(asset_details, true) OVER (
            PARTITION BY ef_fund_id, ef_portfolio_id ORDER BY REPORT_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _filled_asset_details
    FROM na_overridden na
),
forward_filled AS (
    SELECT
        ff.* EXCEPT (
        asset_names,
        reporting_asset_names,
        asset_types,
        reporting_asset_type,
        asset_lifecycle_phases,
        reporting_asset_lifecycle_phase,
        asset_ownership_phases,
        reporting_asset_ownership_phase,
        asset_countries,
        reporting_asset_country,
        asset_technologies,
        reporting_asset_technology,
        asset_details,
        _clean_asset_names, _filled_asset_names,
        _clean_reporting_asset_names, _filled_reporting_asset_names,
        _clean_asset_types, _filled_asset_types,
        _clean_reporting_asset_type, _filled_reporting_asset_type,
        _clean_asset_lifecycle_phases, _filled_asset_lifecycle_phases,
        _clean_reporting_asset_lifecycle_phase, _filled_reporting_asset_lifecycle_phase,
        _clean_asset_ownership_phases, _filled_asset_ownership_phases,
        _clean_reporting_asset_ownership_phase, _filled_reporting_asset_ownership_phase,
        _clean_asset_countries, _filled_asset_countries,
        _clean_reporting_asset_country, _filled_reporting_asset_country,
        _clean_asset_technologies, _filled_asset_technologies,
        _clean_reporting_asset_technology, _filled_reporting_asset_technology,
        _filled_asset_details
        ),
        ff._filled_asset_names AS asset_names,
        ff._filled_reporting_asset_names AS reporting_asset_names,
        ff._filled_asset_types AS asset_types,
        ff._filled_reporting_asset_type AS reporting_asset_type,
        ff._filled_asset_lifecycle_phases AS asset_lifecycle_phases,
        ff._filled_reporting_asset_lifecycle_phase AS reporting_asset_lifecycle_phase,
        ff._filled_asset_ownership_phases AS asset_ownership_phases,
        ff._filled_reporting_asset_ownership_phase AS reporting_asset_ownership_phase,
        ff._filled_asset_countries AS asset_countries,
        ff._filled_reporting_asset_country AS reporting_asset_country,
        ff._filled_asset_technologies AS asset_technologies,
        ff._filled_reporting_asset_technology AS reporting_asset_technology,
        ff._filled_asset_details AS asset_details,
        coalesce((ff._clean_asset_names IS NULL AND ff._filled_asset_names IS NOT NULL) OR (ff._clean_reporting_asset_names IS NULL AND ff._filled_reporting_asset_names IS NOT NULL) OR (ff._clean_asset_types IS NULL AND ff._filled_asset_types IS NOT NULL) OR (ff._clean_reporting_asset_type IS NULL AND ff._filled_reporting_asset_type IS NOT NULL) OR (ff._clean_asset_lifecycle_phases IS NULL AND ff._filled_asset_lifecycle_phases IS NOT NULL) OR (ff._clean_reporting_asset_lifecycle_phase IS NULL AND ff._filled_reporting_asset_lifecycle_phase IS NOT NULL) OR (ff._clean_asset_ownership_phases IS NULL AND ff._filled_asset_ownership_phases IS NOT NULL) OR (ff._clean_reporting_asset_ownership_phase IS NULL AND ff._filled_reporting_asset_ownership_phase IS NOT NULL) OR (ff._clean_asset_countries IS NULL AND ff._filled_asset_countries IS NOT NULL) OR (ff._clean_reporting_asset_country IS NULL AND ff._filled_reporting_asset_country IS NOT NULL) OR (ff._clean_asset_technologies IS NULL AND ff._filled_asset_technologies IS NOT NULL) OR (ff._clean_reporting_asset_technology IS NULL AND ff._filled_reporting_asset_technology IS NOT NULL) OR (ff.asset_details IS NULL AND ff._filled_asset_details IS NOT NULL), false) AS backfilled
    FROM ff_computed ff
)

-- 14. Final output: clean up remaining (non-override) string columns' "" -> NULL, drop the
--     pre-existing load_date_column value (re-stamped fresh below, matching the PySpark
--     .withColumn(load_date_column, ...) overwrite semantics), and stamp the new timestamp
SELECT
    * EXCEPT (
        ef_fund_id,
        ef_portfolio_id,
        ef_fund,
        ef_portfolio,
        cdm_fund_id,
        cdm_portfolio_id,
        cdm_fund,
        cdm_portfolio,
        inv_pipeline_type,
        invp_investment_strategy,
        fund_structure_type_name,
        refresh_timestamp
    ),
    CASE WHEN ef_fund_id = '' THEN NULL ELSE ef_fund_id END AS ef_fund_id,
        CASE WHEN ef_portfolio_id = '' THEN NULL ELSE ef_portfolio_id END AS ef_portfolio_id,
        CASE WHEN ef_fund = '' THEN NULL ELSE ef_fund END AS ef_fund,
        CASE WHEN ef_portfolio = '' THEN NULL ELSE ef_portfolio END AS ef_portfolio,
        CASE WHEN cdm_fund_id = '' THEN NULL ELSE cdm_fund_id END AS cdm_fund_id,
        CASE WHEN cdm_portfolio_id = '' THEN NULL ELSE cdm_portfolio_id END AS cdm_portfolio_id,
        CASE WHEN cdm_fund = '' THEN NULL ELSE cdm_fund END AS cdm_fund,
        CASE WHEN cdm_portfolio = '' THEN NULL ELSE cdm_portfolio END AS cdm_portfolio,
        CASE WHEN inv_pipeline_type = '' THEN NULL ELSE inv_pipeline_type END AS inv_pipeline_type,
        CASE WHEN invp_investment_strategy = '' THEN NULL ELSE invp_investment_strategy END AS invp_investment_strategy,
        CASE WHEN fund_structure_type_name = '' THEN NULL ELSE fund_structure_type_name END AS fund_structure_type_name,
    CURRENT_TIMESTAMP() AS refresh_timestamp
FROM forward_filled
"""

AUM_METRICS_ENRICHED_SQL = AUM_METRICS_ENRICHED_SQL.replace("{LANDING_DB}", SOURCE_DATABASE)

