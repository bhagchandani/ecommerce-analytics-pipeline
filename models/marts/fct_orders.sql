{{
    config(
        materialized='table'
    )
}}

SELECT 
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    o.order_status,
    oi.quantity,
    oi.unit_price,
    oi.total_price,
    p.cost as unit_cost,
    oi.quantity * p.cost as total_cost,
    oi.total_price - (oi.quantity * p.cost) as profit,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_order_items') }} oi
JOIN  {{ ref('stg_orders') }} o
    ON oi.order_id = o.order_id
JOIN {{ ref('stg_products') }} p
    ON oi.product_id = p.product_id
    
WHERe o.order_status = 'completed'