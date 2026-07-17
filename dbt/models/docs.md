{% docs stg_commodity %}
This model builds directly on top of its corresponding commodity source table. It takes raw data and performs very light transformations such as:
cast the price to a numeric type and convert the price_date which is a timestamp into a BigQuery DATE object. It materializes into a view.
It contains the following columns:
- price_date (DATE, contains only unique values)
- price (FLOAT64, the closing price of a commodity on that date)
- commodity_code (STRING, a code used in Oil Price API to represent a certain commodity)

{% enddocs %}

{% docs fct_daily_commodity_prices %}
An incremental model that combines (using UNION) all daily prices from all staging models into a single table. It is strictly numeric and chronological. 
It is materialized into a long, thin fact table containing every price point from every source.
It contains the following columns:
- price_date (DATE, a date column which can contain duplicate values)
- price (FLOAT64, the closing price of a commodity on that date)
- commodity_code (STRING, a code used in Oil Price API to represent a certain commodity, can also be used as a foreign key to join this table with the dimension table) 

{% enddocs %}

{% docs dim_commodities %}
a CSV seed that maps commodity codes to detailed descriptions. It is materialized into a table.
It contains the following columns:
- commodity_code (STRING, a code used in Oil Price API to represent a certain commodity, can also be used as a foreign key to join this table with the dimension table)
- price_origin (STRING, an exchange from where the commodity price is obtained)
- unit (STRING, the primary unit of measurement for a commodity price)
- type (STRING, the type of price: can be either a spot price or a futures contract)
- commodity_description (STRING, a short description of a commodity)
{% enddocs %}

{% docs mart_pivoted %}
A model that turns a long and thin fct_daily_commodity_prices table into a wide table that aligns prices on the same date.
With it we can easily create correlation charts.
Materializes into a view.
It contains the following columns:
- price_date (DATE, a date column which contains only unique values)
- WTI_USD (STRING, a dedicated column for WTI Crude Oil Price for that date, may contain null values)
- BRENT_CRUDE_USD (STRING, a dedicated column for Brent Crude Oil price for that date, may contain null values)
- NATURAL_GAS_USD (STRING, a dedicated column for Natural Gas price for that date, may contain null values)
- UREA_USD (STRING, a dedicated column for Urea price for that date, may contain null values)
{% enddocs %}

{% docs mart_joined %}
A model that joins fct_daily_commodity_prices and dim_commodities into a OBT. 
Basically it's the same fct_daily_commodity_prices model with additional information for each commodity.
Materializes into a view.
It contains the following columns:
- price_date (DATE, a date column which can contain duplicate values)
- price (FLOAT64, the closing price of a commodity on that date)
- commodity_code (STRING, a code used in Oil Price API to represent a certain commodity, used as a key to join fct_daily_commodity_prices and dim_commodities on)
- commodity_name (STRING, a short commodity description)
- unit (STRING, the primary unit of measurement for a commodity price)
- type (STRING, the type of price: can be either a spot price or a futures contract)
{% enddocs %}

{% docs mart_metrics %}
This model builds directly on top of fct_daily_commodity_prices. It is created specifically for calculating market trends over time, 
such as daily returns, volatility and moving averages over a 7-days and a 30-days windows.
Materializes into a view.
It contains the following columns:
- price_date (DATE, a date column which can contain duplicate values)
- price (FLOAT64, the closing price of a commodity on that date)
- commodity_code (STRING, a code used in Oil Price API to represent a certain commodity)
- daily_return_pct (FLOAT, the % change from yesterday's price)
- moving_avg_7d (FLOAT, shows how much the price fluctuates over the last 7 days)
- moving_avg_7d (FLOAT, shows the average closing price over the last 7 days)
- moving_avg_30d (FLOAT, shows how much the price fluctuates over the last 30 days)
- moving_avg_30d (FLOAT, shows the average closing price over the last 30 days)
{% enddocs %}
