select
    product_id,
    product_name,
    category,
    price,
    cost,
    ROUND(price -cost, 2) as profit_margin
from {{ source('raw', 'raw_products')}}