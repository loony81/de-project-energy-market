-- join prices with dim_commodities into a wide mart for analysis
-- Each commodity price has its own line, but it provides more information about each commodity
with prices as (
    select * from {{ ref('fct_daily_commodity_prices') }}
),

metadata as (
    select * from {{ ref('dim_commodities') }}
)

select
    p.price_date,
    p.price,
    m.*
from prices p
left join metadata m 
    on p.commodity_code = m.commodity_code
order by 1 desc
