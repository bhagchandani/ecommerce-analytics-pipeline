select
    order_id,
    customer_id,
    order_date,
    order_status
    
from {{ source('raw', 'raw_orders')}}