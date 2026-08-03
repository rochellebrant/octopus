cdm_funds = '''
select distinct
    c.fund_core_id,
    n.fund_display_name,
    n.fund_legal_name
  from oegen_data_prod_prod.core_data_model.bronze_fund_dim_names n
  left join oegen_data_prod_prod.core_data_model.bronze_fund_dim_core c
  on n.fund_name_id=c.primary_fund_name_id
  '''