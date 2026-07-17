-- If this query returns any rows, the test fails.
select
    *
from {{ ref('mart_metrics') }}
where 
    (short_term_trend = 'Short-term Bullish' and price < moving_avg_7d)
    or 
    (short_term_trend = 'Short-term Bearish' and price > moving_avg_7d)
    or 
    (long_term_trend = 'Long-term Bearish' and price > moving_avg_30d)
    or
    (long_term_trend = 'Long-term Bullish' and price < moving_avg_30d)
