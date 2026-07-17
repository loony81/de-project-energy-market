{% test negative_price_check(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where price <= 0
{% endtest %}
