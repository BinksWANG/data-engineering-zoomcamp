{{
    config(
        materialized='table'
    )
}}

with trip_durations as (
    select
        PUlocationID as pickup_location_id,
        DOlocationID as dropoff_location_id,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
    where pickup_datetime is not null
      and dropOff_datetime is not null
      and PUlocationID is not null
      and DOlocationID is not null
),

percentiles as (
    select
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        percentile_cont(trip_duration, 0.90) over (partition by year, month, pickup_location_id, dropoff_location_id) as p90_trip_duration
    from trip_durations
),

distinct_percentiles as (
    select distinct
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        p90_trip_duration
    from percentiles
)

select
    pickup_location_id,
    dropoff_location_id,
    year,
    month,
    p90_trip_duration
from distinct_percentiles
order by pickup_location_id, dropoff_location_id, year, month