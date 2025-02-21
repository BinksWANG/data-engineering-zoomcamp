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
        approx_quantiles(fare_amount, 100)[offset(95)] as p95_fare_amount,
        approx_quantiles(fare_amount, 100)[offset(97)] as p97_fare_amount,
        approx_quantiles(fare_amount, 100)[offset(90)] as p90_fare_amount
    from valid_trips
    group by 1, 2, 3
)

select
    service_type,
    year,
    month,
    p95_fare_amount,
    p97_fare_amount,
    p90_fare_amount
from percentiles
order by service_type, year, month