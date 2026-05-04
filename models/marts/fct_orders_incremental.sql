{{
  config(
    materialized='incremental',
    unique_key='order_item_id'
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
    p.cost AS unit_cost,
    oi.quantity * p.cost AS total_cost,
    oi.total_price - (oi.quantity * p.cost) AS profit,
    CURRENT_TIMESTAMP() AS updated_at
    
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id

WHERE o.order_status = 'completed'

{% if is_incremental() %}
    -- Only process new orders since last run
    AND o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}