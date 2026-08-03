cdm_companies = '''
select
  comp.company_core_id,
  comp.company_registered_name,
  comp.company_registered_incorporation_number,
  comp.company_type_id,
  typ.company_type_name,
  typ.company_type_description,
  comp.company_lifecycle_id,
  comp.validation_stage,
  comp.deal_name,
  comp.company_sec_vcorp,
  comp.incorporation_date,
  comp.vat_number,
  comp.certificate_of_incorporation_link,
  comp.companies_house_link,
  comp.filing_code_hide,
  comp.signers,
  comp.authorised,
  comp.registered_office_address,
  comp.nature,
  comp.accountant_auditor,
  comp.nar_next_annual_reports,
  comp.year_end,
  comp.administration_management_counsel,
  comp.post,
  comp.dir_address,
  comp.psc,
  comp.country_core_id,
  comp.country_groups_id,
  cntr.country_name,
  cntr.continent,
  cntr.region,
  cntr.country_code_2_letter,
  cntr.currency_name,
  cntr.currency_iso_code,
  cntr.currency_symbol_ascii_code,
  comp.technology_name_id,
  t.technology_name
from oegen_data_prod_prod.core_data_model.bronze_company_dim_core comp
left join oegen_data_prod_prod.core_data_model.bronze_company_dim_type typ on comp.company_type_id = typ.company_type_id
left join oegen_data_prod_prod.core_data_model.bronze_country_dim_core cntr on comp.country_core_id = cntr.country_core_id
left join oegen_data_prod_prod.core_data_model.bronze_technology_dim_names t on t.technology_name_id = comp.technology_name_id
'''