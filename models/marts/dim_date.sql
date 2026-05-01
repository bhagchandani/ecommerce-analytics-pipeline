{{
    config(
        materialized="table"
    )
}}

WITH date_spine AS (
    SELECT 
        DATEADD(day, seq4(), '2022-01-01'::date) as date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1095))
)

SELECT
    date_day,
    YEAR(date_day) as year,
    QUARTER(date_day) as quarter,
    MONTH(date_day) as month,
    MONTHNAME(date_day) as month_name,
    DAY(date_day) as day,
    DAYOFWEEK(date_day) as day_of_week,
    DAYNAME(date_day) as day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (0,6) THEN TRUE ELSE FALSE END as is_weekend,
FROM date_spine