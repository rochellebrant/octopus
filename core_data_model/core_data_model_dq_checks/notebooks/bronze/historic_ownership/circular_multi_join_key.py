# Databricks notebook source
# DBTITLE 1,View
table_name = dbutils.widgets.get("table_name")
check_name = dbutils.widgets.get("check_name")
issues_fqtn = dbutils.widgets.get("issues_fqtn")

source_db = f"{dbutils.widgets.get('source_catalog')}.{dbutils.widgets.get('source_schema')}"

print(f"Running bespoke check: {check_name} for table: {table_name}")
print(f"Creating/Updating LIVE VIEW at: {issues_fqtn}")

# =================================================================================
# DYNAMIC SQL VIEW GENERATION
# =================================================================================

sql_query = f"""
CREATE OR REPLACE VIEW {issues_fqtn} AS 

WITH 
-- ==========================================
-- 1. ASSETS & ASSET MAPPING
-- ==========================================
bronze_assets AS (
    SELECT company_core_id AS asset_dim__company_id, asset_infra_id AS asset_dim__asset_id
    FROM {source_db}.bronze_asset_infra_dim_core 
    WHERE END_AT IS NULL
    
    UNION ALL
    
    SELECT company_core_id AS asset_dim__company_id, asset_plat_id AS asset_dim__asset_id
    FROM {source_db}.bronze_asset_platform_dim_core 
    WHERE END_AT IS NULL
),

bronze_asset_invp AS (
    SELECT asset_id AS asst_invp_map__asset_id, investment_portfolio_id AS asst_invp_map__invp_id
    FROM {source_db}.bronze_asset_invp
),

step_one AS (
    SELECT 
        a.asset_dim__company_id, a.asset_dim__asset_id,
        m.asst_invp_map__asset_id, m.asst_invp_map__invp_id,
        CASE 
            WHEN m.asst_invp_map__asset_id IS NULL THEN CONCAT('Missing in bronze_asset_invp. Expected asset_id: ', CAST(a.asset_dim__asset_id AS STRING))
            WHEN a.asset_dim__asset_id IS NULL THEN CONCAT('Missing in bronze_asset_*_dim_core. bronze_asset_invp references orphaned asset_id: ', CAST(m.asst_invp_map__asset_id AS STRING))
            ELSE NULL 
        END AS misjoin_asset_to_map
    FROM bronze_assets a
    FULL OUTER JOIN bronze_asset_invp m ON a.asset_dim__asset_id = m.asst_invp_map__asset_id
),

-- ==========================================
-- 2. INVESTMENT PORTFOLIO INTERNAL
-- ==========================================
invp_dim AS (
    SELECT investment_portfolio_id AS invp_dim__invp_id, invp_version_id AS invp_dim__version_id, invp_fund_core_id AS invp_dim__fund_id
    FROM {source_db}.bronze_investment_portfolio_dim_core_af
    WHERE END_AT IS NULL
),

invp_bridge AS (
    SELECT investment_portfolio_id AS invp_brg__invp_id, invp_version_id AS invp_brg__version_id
    FROM {source_db}.bronze_investment_portfolio_bridge
    WHERE END_AT IS NULL
),

invp_fact AS (
    SELECT invp_version_id AS invp_vers_version_id
    FROM {source_db}.bronze_investment_portfolio_fact_version
    WHERE END_AT IS NULL
),

invp_canonical AS (
    SELECT 
        d.invp_dim__invp_id, d.invp_dim__version_id, d.invp_dim__fund_id,
        b.invp_brg__invp_id, b.invp_brg__version_id,
        f.invp_vers_version_id,
        CASE 
            WHEN d.invp_dim__invp_id IS NULL AND b.invp_brg__invp_id IS NOT NULL THEN CONCAT('Missing in INVP Dim Core. Bridge references orphaned Portfolio: ', CAST(b.invp_brg__invp_id AS STRING), ' / Version: ', CAST(b.invp_brg__version_id AS STRING))
            WHEN b.invp_brg__invp_id IS NULL AND d.invp_dim__invp_id IS NOT NULL THEN CONCAT('Missing in INVP Bridge. Dim Core references orphaned Portfolio: ', CAST(d.invp_dim__invp_id AS STRING), ' / Version: ', CAST(d.invp_dim__version_id AS STRING))
            WHEN f.invp_vers_version_id IS NULL AND COALESCE(d.invp_dim__version_id, b.invp_brg__version_id) IS NOT NULL THEN CONCAT('Missing in INVP Fact Version. Missing invp_version_id: ', CAST(COALESCE(d.invp_dim__version_id, b.invp_brg__version_id) AS STRING))
            WHEN f.invp_vers_version_id IS NOT NULL AND COALESCE(d.invp_dim__version_id, b.invp_brg__version_id) IS NULL THEN CONCAT('Orphan in INVP Fact Version. Fact references version not in Dim/Bridge: ', CAST(f.invp_vers_version_id AS STRING))
            ELSE NULL 
        END AS misjoin_invp_internal,
        COALESCE(d.invp_dim__invp_id, b.invp_brg__invp_id) AS coalesced_invp_id
    FROM invp_dim d
    FULL OUTER JOIN invp_bridge b 
        ON d.invp_dim__invp_id = b.invp_brg__invp_id AND d.invp_dim__version_id = b.invp_brg__version_id
    FULL OUTER JOIN invp_fact f 
        ON COALESCE(d.invp_dim__version_id, b.invp_brg__version_id) = f.invp_vers_version_id
),

-- ==========================================
-- 3. ASSET MAP TO INVP CANONICAL
-- ==========================================
step_two AS (
    SELECT 
        s1.*, 
        ic.*,
        CASE 
            WHEN s1.asst_invp_map__invp_id IS NOT NULL AND ic.coalesced_invp_id IS NULL THEN CONCAT('Missing in INVP Canonical Structure. bronze_asset_invp references missing investment_portfolio_id: ', CAST(s1.asst_invp_map__invp_id AS STRING))
            WHEN s1.asst_invp_map__invp_id IS NULL AND ic.coalesced_invp_id IS NOT NULL THEN CONCAT('Missing in bronze_asset_invp. No asset mapped to investment_portfolio_id: ', CAST(ic.coalesced_invp_id AS STRING))
            ELSE NULL 
        END AS misjoin_asst_invp_map_to_invp
    FROM step_one s1
    FULL OUTER JOIN invp_canonical ic ON s1.asst_invp_map__invp_id = ic.coalesced_invp_id
),

-- ==========================================
-- 4. INVP TO FUND
-- ==========================================
fund_dim AS (
    SELECT fund_core_id AS fund_dim_fund_id, company_core_id AS fund_dim_company_id
    FROM {source_db}.bronze_fund_dim_core
    WHERE END_AT IS NULL
),

step_three AS (
    SELECT 
        s2.*, 
        fd.*,
        CASE 
            WHEN s2.invp_dim__fund_id IS NOT NULL AND fd.fund_dim_fund_id IS NULL THEN CONCAT('Missing in bronze_fund_dim_core. INVP Dim references orphaned invp_fund_core_id: ', CAST(s2.invp_dim__fund_id AS STRING))
            ELSE NULL 
        END AS misjoin_invp_to_fund
    FROM step_two s2
    FULL OUTER JOIN fund_dim fd ON s2.invp_dim__fund_id = fd.fund_dim_fund_id
),

-- ==========================================
-- 5. FUND/ASSET RELATIONSHIP MAP
-- ==========================================
rel_map AS (
    SELECT DISTINCT ultimate_parent_id AS rels_ultimate_parent_id, ultimate_child_id AS rels_ultimate_child_id
    FROM {source_db}.bronze_company_parent_relationship_map
    WHERE END_AT IS NULL
),

step_four AS (
    SELECT 
        s3.*, 
        rm.*,
        CASE 
            WHEN s3.asset_dim__company_id IS NOT NULL AND s3.fund_dim_company_id IS NOT NULL AND rm.rels_ultimate_parent_id IS NULL THEN CONCAT('Missing in Rel Map. Expected link between Parent/Fund: ', CAST(s3.fund_dim_company_id AS STRING), ' and Child/Asset: ', CAST(s3.asset_dim__company_id AS STRING))
            WHEN s3.asset_dim__company_id IS NULL AND s3.fund_dim_company_id IS NULL AND rm.rels_ultimate_parent_id IS NOT NULL THEN CONCAT('Orphan in Rel Map. Map references missing IDs - Parent: ', CAST(rm.rels_ultimate_parent_id AS STRING), ', Child: ', CAST(rm.rels_ultimate_child_id AS STRING))
            ELSE NULL 
        END AS misjoin_circle_to_rel_map
    FROM step_three s3
    FULL OUTER JOIN rel_map rm 
        ON s3.fund_dim_company_id = rm.rels_ultimate_parent_id AND s3.asset_dim__company_id = rm.rels_ultimate_child_id
),

-- ==========================================
-- 6. FINAL PROJECTION (Filter to Issues Only)
-- ==========================================
final_output AS (
    SELECT 
        -- Coalesced Master IDs
        COALESCE(asset_dim__asset_id, asst_invp_map__asset_id) AS final_asset_id,
        COALESCE(invp_dim__invp_id, invp_brg__invp_id, asst_invp_map__invp_id) AS final_invp_id,
        COALESCE(invp_dim__version_id, invp_brg__version_id, invp_vers_version_id) AS final_invp_version_id,
        COALESCE(fund_dim_fund_id, invp_dim__fund_id) AS final_fund_id,
        COALESCE(asset_dim__company_id, rels_ultimate_child_id) AS final_asset_company_id,
        COALESCE(fund_dim_company_id, rels_ultimate_parent_id) AS final_fund_company_id,
        
        -- Underlying IDs
        asset_dim__asset_id, asset_dim__company_id,
        asst_invp_map__asset_id, asst_invp_map__invp_id,
        invp_dim__invp_id, invp_dim__version_id, invp_brg__invp_id, invp_brg__version_id, invp_vers_version_id, invp_dim__fund_id,
        fund_dim_fund_id, fund_dim_company_id,
        rels_ultimate_parent_id, rels_ultimate_child_id,

        -- Issue Tracking Columns
        misjoin_asset_to_map,
        misjoin_invp_internal,
        misjoin_asst_invp_map_to_invp,
        misjoin_invp_to_fund,
        misjoin_circle_to_rel_map,
        
        -- Master Issue Text
        COALESCE(
            misjoin_asset_to_map, 
            misjoin_invp_internal, 
            misjoin_asst_invp_map_to_invp, 
            misjoin_invp_to_fund, 
            misjoin_circle_to_rel_map
        ) AS issue
    FROM step_four
)

SELECT * FROM final_output WHERE issue IS NOT NULL;
"""

# Execute the SQL Query to create the view
spark.sql(sql_query)
print(f"Success! View {issues_fqtn} created. It will now reflect real-time data.")

# COMMAND ----------

