# Note that we calculate this table base on *all* records in the bronze scd layer, regardless of __end_at status. 
# In the next step, we MERGE INTO our silver table, so we only bring in those records which are 'new'
silver_fundoperations_by_investor_sql_code = '''
WITH
enriched_fo AS (
  SELECT
    Ef_Fund_Id,
    ef_Fund,
    Fund,
    Fund_Structure_Type,
    Fund_Status,
    Investor,
    Investor_Type,
    Investor_Country,
    Effective_Date,
    Transaction_Currency,
    Commitments,
    Called,
    Distribution,
    NAV,
    datalake_ingestion_timestamp
  FROM {silver_prefix}fundoperations_daily fo
)
, enr_cumulatives1 AS (
  SELECT
    enr.*,
    
    -- cumulative (to-date)
    SUM(enr.Commitments)  OVER w_td   AS Cumulative_Commitments,
    SUM(enr.Called)       OVER w_td   AS Cumulative_Called, 
    SUM(enr.Distribution) OVER w_td   AS Cumulative_Distribution
  FROM enriched_fo enr
  WINDOW
  w_td AS (
    PARTITION BY enr.Investor, enr.Investor_Type, enr.Investor_Country, enr.Transaction_Currency 
    ORDER BY enr.Effective_Date ASC, enr.Ef_Fund_Id ASC 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
SELECT
  *,
  (e1.Cumulative_Commitments-e1.Cumulative_Called) AS Remaining_Commitments,
  CURRENT_TIMESTAMP() AS refresh_timestamp
FROM enr_cumulatives1 e1
ORDER BY Investor, Investor_Type, Investor_Country, Transaction_Currency, Effective_Date, Ef_Fund_Id
'''