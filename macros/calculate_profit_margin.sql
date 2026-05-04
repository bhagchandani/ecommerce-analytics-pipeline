{% macro calculate_profit_margin(price_column, cost_column) %}
    ROUND(
        (({{ price_column }} - {{ cost_column }}) / {{ price_column }}) * 100,
        2
    )
{% endmacro %}