-- instead of overwriting the table every time a new row of data comes in,
-- I implemented an incremental materialization to insert only new or updated records since the last run
-- without completely dropping and recreating this table
{{
  config(
    materialized='incremental',
    unique_key=['price_date', 'commodity_code'],
    incremental_strategy='merge'
  )
}}
select * from {{ ref('stg_wti_oil') }}
union all
select * from {{ ref('stg_brent_oil') }}
union all
select * from {{ ref('stg_nat_gas') }}
union all
select * from {{ ref('stg_urea') }}

{% if is_incremental() %}
where  price_date >= (select max(price_date) from {{ this }})
{% endif %}
