{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        'FHV' as service_type
    from {{ ref('stg_ext_fhv') }}
), 
trips_unioned as (
    select * from fhv_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.dispatching_base_num,
    trips_unioned.pickup_datetime,
    trips_unioned.dropOff_datetime,
    trips_unioned.PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.SR_Flag,
    trips_unioned.Affiliated_base_number,
    extract(year from trips_unioned.pickup_datetime) as year,
    extract(month from trips_unioned.pickup_datetime) as month
from trips_unioned
inner join dim_zones as pickup_zone
    on trips_unioned.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on trips_unioned.DOlocationID = dropoff_zone.locationid
