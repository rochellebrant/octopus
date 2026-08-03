gold_mandate_to_company_fact_view_sql_code = """
select 
    ultimate_parent_company_id as mandate_company_id,
    ultimate_child_company_id as company_id,
    ultimate_parent_company_name as mandate_name,
    ultimate_child_company_name as company_name,
    source,
    percentage_ownership,
    transaction_type,
    active_from_date,
    active_to_date,
    cdm_investment_portfolio_ids,
    cdm_investment_portfolio_names,
    cdm_fund_ids,
    cdm_fund_names,
    refresh_timestamp
from {gold_prefix}company_relationships_fact
where ultimate_parent_company_id in (select company_core_id from oegen_data_prod_prod.core_data_model.bronze_fund_dim_core where END_AT IS NULL)
"""