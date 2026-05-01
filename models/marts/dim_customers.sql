{{
    config(
        materialized='table'
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    signup_date,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_customers')}}