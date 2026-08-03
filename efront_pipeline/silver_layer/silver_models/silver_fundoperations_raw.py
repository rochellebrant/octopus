# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_raw_sql_code = '''
WITH
-- Deduplicate base operations
clean_fundoperations AS (
    SELECT * FROM {bronze_prefix}fundoperations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Transaction_Investor_IQId ORDER BY datalake_ingestion_timestamp DESC) = 1
),

-- Combine, trim, and deduplicate the fund mapping logic
clean_mapped_funds AS (
    SELECT
        fb.source_fund_id,
        fdn.fund_display_name AS cdm_fund_short_name,
        fdn.fund_legal_name AS cdm_fund_legal_name
    FROM oegen_data_prod_prod.core_data_model.bronze_mapping_fund fb
    JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc 
        ON fb.cdm_fund_id = fdc.fund_core_id AND fdc.END_AT IS NULL
    JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn 
        ON fdn.fund_name_id = fdc.primary_fund_name_id AND fdn.END_AT IS NULL
    JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_structure_type fst 
        ON fst.fund_structure_type_id = fdc.fund_structure_type_id AND fst.END_AT IS NULL
    WHERE UPPER(fb.source_system_id) = 'SRCE_SYST_1001' AND fb.END_AT IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fb.source_fund_id ORDER BY fb.START_AT DESC) = 1
)

SELECT  
    fo.Fund_IQId                AS ef_fund_id,
    mf.cdm_fund_short_name      AS fund_display_name,
    mf.cdm_fund_legal_name      AS fund_legal_name,
    fo.Investor_Account         AS investor_account,
    fo.Share                    AS share,
    fo.Type                     AS type,
    
    date_format(fo.Effective_Date, 'yyyy-MM-dd') AS effective_date,
    fo.Currency                 AS fund_currency,
    fo.Quantity                 AS units,
    fo.Draft                    AS draft,
    
    COALESCE(fo.Redrawable_amount__fund_curr_, 0) AS redrawable_amount_fund_curr,
    COALESCE(
        NULLIF(fo.Amount__fund_curr_, 0),
        NULLIF(fo.Committed_amount__fund_curr_, 0),
        NULLIF(fo.Valuation__fund_curr_, 0),
        NULLIF(fo.FundExpired_Original_Commitment, 0),
        0
    ) AS amount_fund_curr,
    
    fo.`_Unit_Price`            AS unit_price,
    fo.Created_on               AS created_on,
    fo.Modified_by              AS modified_by,
    fo.Modified_on              AS modified_on,
    
    fo.datalake_ingestion_timestamp,
    CURRENT_TIMESTAMP()         AS refresh_timestamp
    
FROM clean_fundoperations fo
LEFT JOIN clean_mapped_funds mf 
    ON fo.Fund_IQId = mf.source_fund_id
WHERE
    fo.Share != 'Top 20'
    AND fo.Draft != 'True'
ORDER BY 
    fo.Effective_Date DESC
'''