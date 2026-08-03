efront_portfolios_aum_fact = '''
SELECT DISTINCT
  aum.PORTFOLIO_NAME
FROM {bronze_prefix}aum_asset aum'''