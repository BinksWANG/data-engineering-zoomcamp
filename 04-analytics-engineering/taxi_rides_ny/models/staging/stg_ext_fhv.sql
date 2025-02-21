with 

source as (

    select * from {{ source('staging', 'ext_fhv') }}

),

renamed as (

    select 
        *
    from source

)

select * from renamed
where dispatching_base_num is not null