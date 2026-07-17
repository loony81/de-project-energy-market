-- We pivot the prices into columns using the commodity_code
-- to get one row per date with all prices aligned
-- With it we can easily create correlation charts
with prices as (
    select * from {{ ref('fct_daily_commodity_prices') }}
),

metadata as (
    select * from {{ ref('dim_commodities') }}
)

select * from
  (select price_date, price, commodity_code FROM prices)
  pivot(sum(price) for commodity_code in ('WTI_USD', 'BRENT_CRUDE_USD', 'NATURAL_GAS_USD', 'UREA_USD'))
  order by 1
