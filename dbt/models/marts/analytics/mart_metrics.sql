with daily_prices as (
    select * from {{ ref('fct_daily_commodity_prices') }}
),

calculations as (
    select
        price_date,
        commodity_code,
        price,
        -- Get yesterday's price to calculate daily return
        lag(price) over (partition by commodity_code order by price_date) as prev_day_price,
        -- 7-Day Moving Average
        avg(price) over (
            partition by commodity_code 
            order by price_date 
            rows between 6 preceding and current row
        ) as moving_avg_7d,
        
        -- 7-Day Standard Deviation (Volatility)
        stddev(price) over (
            partition by commodity_code 
            order by price_date 
            rows between 6 preceding and current row
        ) as volatility_7d,
        -- 30-Day Moving Average
        avg(price) over (
            partition by commodity_code 
            order by price_date 
            rows between 29 preceding and current row
        ) as moving_avg_30d,

        -- 30-Day Standard Deviation (Volatility)
        stddev(price) over (
            partition by commodity_code 
            order by price_date 
            rows between 29 preceding and current row
        ) as volatility_30d
    from daily_prices
)

select
    *,
    -- Calculate % Change over 1 day
    round(safe_divide((price - prev_day_price), prev_day_price) * 100, 2) as daily_return_pct,
    -- Trend logic based on the 7-day window
    case 
        when price > moving_avg_7d then 'Short-term Bullish'
        when price < moving_avg_7d then 'Short-term Bearish'
        else 'Neutral'
    end as short_term_trend,
    -- Trend logic based on the 30-day window
    case 
        when price > moving_avg_30d then 'Long-term Bullish'
        when price < moving_avg_30d then 'Long-term Bearish'
        else 'Neutral'
    end as long_term_trend
from calculations
