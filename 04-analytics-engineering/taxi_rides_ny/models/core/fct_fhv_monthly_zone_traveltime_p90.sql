with trip_data as (
    select 
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID as pickup_locationid,
        DOlocationID as dropoff_locationid,
        SR_Flag,
        Affiliated_base_number,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        -- Calculate trip_duration in seconds
        timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select 
    year,
    month,
    pickup_locationid,
    dropoff_locationid,
    -- Calculate the p90 of trip_duration
    percentile_cont(trip_duration, 0.90) over (
        partition by year, month, pickup_locationid, dropoff_locationid
    ) as p90_trip_duration
from trip_data
