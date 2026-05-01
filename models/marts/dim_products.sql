{{
    config(
        materialized="table"
    )
}}

SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    profit_margin,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref("stg_products") }}