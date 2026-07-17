--this custom macro extracts necessary columns from source tables in the 'raw' schema and also discards any duplicates
-- if there are any rows with the same date, we group data into buckets by date, then inside each bucket, we sort the rows so that the most recent timestamp is at the very top
-- then select the very first row which is the latest price
-- thus preventing populating our staging models with duplicate values
{% macro extract_data_from_sources(schema_name,table_name) %}
  select code as commodity_code, 
  date(updated_at) as price_date, 
  SAFE_CAST(price AS FLOAT64) as price from  {{ source(schema_name, table_name) }}
  qualify row_number() over (partition by date(updated_at) order by updated_at desc) = 1 --select only the latest price in case there are more than one row for the same date
{% endmacro %}
