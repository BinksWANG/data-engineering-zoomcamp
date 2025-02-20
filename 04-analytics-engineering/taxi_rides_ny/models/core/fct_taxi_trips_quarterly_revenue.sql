{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('dim_taxi_trips') }}
),

quarterly_revenue as (
    select
        -- Group by year, quarter, and service type
        extract(year from pickup_datetime) as revenue_year,
        extract(quarter from pickup_datetime) as revenue_quarter,
        concat(
            extract(year from pickup_datetime), 
            '/Q', 
            extract(quarter from pickup_datetime)
        ) as year_quarter,
        service_type,

        -- Revenue calculation
        sum(fare_amount) as revenue_quarterly_fare,
        sum(extra) as revenue_quarterly_extra,
        sum(mta_tax) as revenue_quarterly_mta_tax,
        sum(tip_amount) as revenue_quarterly_tip_amount,
        sum(tolls_amount) as revenue_quarterly_tolls_amount,
        sum(ehail_fee) as revenue_quarterly_ehail_fee,
        sum(improvement_surcharge) as revenue_quarterly_improvement_surcharge,
        sum(total_amount) as revenue_quarterly_total_amount,

        -- Additional calculations
        count(tripid) as total_quarterly_trips,
        avg(passenger_count) as avg_quarterly_passenger_count,
        avg(trip_distance) as avg_quarterly_trip_distance

    from trips_data
    group by 1, 2, 3, 4
),

yoy_growth as (
    select
        revenue_year,
        revenue_quarter,
        year_quarter,
        service_type,
        revenue_quarterly_total_amount,
        lag(revenue_quarterly_total_amount) over (
            partition by service_type, revenue_quarter 
            order by revenue_year
        ) as previous_year_revenue,
        round(
            (revenue_quarterly_total_amount - lag(revenue_quarterly_total_amount) over (
                partition by service_type, revenue_quarter 
                order by revenue_year
            )) / NULLIF(lag(revenue_quarterly_total_amount) over (
                partition by service_type, revenue_quarter 
                order by revenue_year
            ), 0) * 100, 2
        ) as yoy_growth_percentage
    from quarterly_revenue
)

select
    revenue_year,
    revenue_quarter,
    year_quarter,
    service_type,
    revenue_quarterly_total_amount,
    previous_year_revenue,
    yoy_growth_percentage
from yoy_growth
order by revenue_year, revenue_quarter, service_type