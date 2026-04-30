SELECT
    customer_id,
    first_name,
    last_name,
    LOWER(TRIM(email)) as email,
    country,
    signup_date
FROM {{ source('raw', 'raw_customers')}}
where email is not null