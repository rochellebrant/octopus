# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_sql_code = '''
WITH
-- Deduplicate base operations (Replace 'Operation_ID' with actual Primary Key)
clean_fundoperations AS (
    SELECT * FROM {bronze_prefix}fundoperations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Transaction_Investor_IQId ORDER BY datalake_ingestion_timestamp DESC) = 1
)
-- Deduplicate base funds
, clean_fund AS (
    SELECT * FROM {bronze_prefix}fund
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Fund_IQId ORDER BY datalake_ingestion_timestamp DESC) = 1
)
-- Deduplicate investors
, clean_investoraccount AS (
    SELECT * FROM {bronze_prefix}investoraccount
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Investor_Account, Fund_IQId ORDER BY datalake_ingestion_timestamp DESC) = 1
)
, cdm_efront_mapped_funds AS (
  SELECT
    fb.*,
    fdn.fund_display_name AS CDM_Fund_Short_Name,
    fdn.fund_legal_name AS CDM_Fund_Legal_Name,
    fst.fund_structure_type_name AS CDM_Fund_Structure_Type,
    CASE
      WHEN fdc.fund_end_date IS NULL THEN 'Active'
      ELSE 'Inactive'
  END AS `CDM_Fund_Active_Status`
  FROM oegen_data_prod_prod.core_data_model.bronze_mapping_fund fb
  LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_core fdc ON fb.cdm_fund_id = fdc.fund_core_id
  LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_names fdn ON fdn.fund_name_id = fdc.primary_fund_name_id
  LEFT JOIN oegen_data_prod_prod.core_data_model.bronze_fund_dim_structure_type fst ON fst.fund_structure_type_id = fdc.fund_structure_type_id
  WHERE UPPER(fb.source_system_id) = "SRCE_SYST_1001"
  AND fdc.END_AT IS NULL
  AND fdn.END_AT IS NULL
  AND fst.END_AT IS NULL
)
, clean_mapped_funds AS (
    -- This guarantees exactly one active mapping per source fund
    SELECT *
    FROM cdm_efront_mapped_funds
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_fund_id ORDER BY last_updated DESC) = 1
)
, fund_base AS (
    SELECT  
        fo.Fund_IQId,
        f.Short_Name AS Fund_Short_Name,
        fo.Fund AS Fund_Long_Name,
        mf.CDM_Fund_Short_Name,
        mf.CDM_Fund_Legal_Name,
        mf.CDM_Fund_Structure_Type,
        mf.CDM_Fund_Active_Status,
        fo.Currency AS Transaction_Currency,
        fo.Investor_Account,

        CASE 
            WHEN (TRIM(ia.Investor_Type) = '' OR ia.Investor_Type IS NULL) AND fo.Investor_Account IN (SELECT Fund FROM clean_fund)
            THEN 'Feeder Fund'
            ELSE ia.Investor_Type
        END AS ia_Investor_Type,
        
        CASE 
            WHEN (TRIM(ia.Country) = '' OR ia.Country IS NULL) AND fo.Investor_Account IN (SELECT Fund FROM clean_fund)
            THEN 'N/A'   
            ELSE ia.Country
        END AS ia_Country,

        fo.Effective_Date,
        COALESCE(fo.Committed_amount__fund_curr_, 0) AS Committed_amount,
        COALESCE(fo.Redrawable_amount__fund_curr_, 0) AS Redrawable_amount,
        COALESCE(fo.FundExpired_Original_Commitment, 0) AS Expired_commitment,
        fo.Type,
        COALESCE(fo.Amount__fund_curr_, 0) AS Amount,
        COALESCE(fo.Amount__fund_curr_, 0) AS Distribution_amount,
        COALESCE(fo.Valuation__fund_curr_, 0) AS NAV_amount,
        fo.datalake_ingestion_timestamp
    FROM clean_fundoperations fo
    LEFT JOIN clean_fund f 
        ON fo.Fund_IQId = f.Fund_IQId
    LEFT JOIN clean_investoraccount ia 
        ON ia.Investor_Account = fo.Investor_Account AND ia.Fund_IQId = fo.Fund_IQId
    LEFT JOIN clean_mapped_funds mf 
        ON mf.source_fund_id = f.Fund_IQId
    WHERE
      fo.Share != "Top 20"
      AND fo.Draft != "True"
)
, calc_base1 AS (
    SELECT
      Fund_IQId,
      Fund_Short_Name,
      Fund_Long_Name,
      CDM_Fund_Short_Name,
      CDM_Fund_Legal_Name,
      CDM_Fund_Structure_Type,
      CDM_Fund_Active_Status,
      Transaction_Currency,
      Investor_Account,
      ia_Investor_Type AS Investor_Type,
      ia_Country AS Investor_Country,
      date_format(Effective_Date, 'yyyy-MM-dd') AS Effective_Date,
      
      SUM(CASE 
            WHEN Type = 'MF: Commitment' THEN (Committed_amount + Redrawable_amount)
            WHEN Type = 'MF: Expired/Released Commitment' THEN -Expired_commitment
            ELSE 0 
          END) AS Total_Commitments,
          
      SUM(CASE WHEN Type IN ('MF: Call', 'MF: Return Of Call (Negative Call)') THEN Amount ELSE 0 END) AS Total_Called,
      SUM(CASE WHEN Type = 'MF: Distribution' THEN Distribution_amount ELSE 0 END) AS Total_Distribution,
      SUM(CASE WHEN Type = 'MF: Net Asset Value' THEN NAV_amount ELSE 0 END) AS NAV,
      MAX(datalake_ingestion_timestamp) AS datalake_ingestion_timestamp
    FROM fund_base
    GROUP BY 
      Fund_IQId, Fund_Short_Name, Fund_Long_Name, CDM_Fund_Short_Name, 
      CDM_Fund_Legal_Name, CDM_Fund_Structure_Type, CDM_Fund_Active_Status, 
      Transaction_Currency, Investor_Account, ia_Investor_Type, ia_Country, Effective_Date
)
, calc_base2 AS (
  SELECT
    c1.*,
    -- cumulative (to-date)
    SUM(c1.Total_Commitments) OVER w_td AS Cumulative_Commitments,
    SUM(c1.Total_Called) OVER w_td AS Cumulative_Called, 
    SUM(c1.Total_Distribution) OVER w_td AS Cumulative_Distribution
  FROM calc_base1 c1
  WINDOW
    w_td AS (PARTITION BY c1.Fund_IQId, c1.Transaction_Currency, c1.Investor_Account, c1.Investor_Type, c1.Investor_Country ORDER BY c1.Effective_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ORDER BY CDM_Fund_Short_Name, Investor_Account, Effective_Date
)
SELECT
  c2.Fund_IQId                AS Ef_Fund_Id,
  c2.Fund_Short_Name          AS Ef_Fund,
  c2.Fund_Long_Name           AS Ef_Fund_Long,
  c2.CDM_Fund_Short_Name      AS Fund,
  c2.CDM_Fund_Structure_Type  AS Fund_Structure_Type,
  c2.CDM_Fund_Active_Status   AS Fund_Status,
  c2.Investor_Account         AS Investor,
  c2.Investor_Type,
  c2.Investor_Country,
  c2.Effective_Date,
  c2.Transaction_Currency,
  c2.Total_Commitments        AS Commitments,
  c2.Total_Called             AS Called,
  c2.Total_Distribution       AS Distribution,
  c2.NAV,
  c2.Cumulative_Commitments,
  c2.Cumulative_Called,
  (c2.Cumulative_Commitments - c2.Cumulative_Called) AS Remaining_Commitments,
  c2.Cumulative_Distribution,
  c2.datalake_ingestion_timestamp,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM calc_base2 c2
'''