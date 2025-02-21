{{
    config(
        materialized='table'
    )
}}

with valid_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('dim_taxi_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        percentile_cont(fare_amount, 0.95) over (partition by service_type, year, month) as p95_fare_amount,
        percentile_cont(fare_amount, 0.97) over (partition by service_type, year, month) as p97_fare_amount,
        percentile_cont(fare_amount, 0.90) over (partition by service_type, year, month) as p90_fare_amount
    from valid_trips
),

distinct_percentiles as (
    select distinct
        service_type,
        year,
        month,
        p95_fare_amount,
        p97_fare_amount,
        p90_fare_amount
    from percentiles
)

select
    service_type,
    year,
    month,
    p95_fare_amount,
    p97_fare_amount,
    p90_fare_amount
from distinct_percentiles
order by service_type, year, month