with

source as (

    select * from {{ source('jaffle_shop', 'locations') }}

    -- if you generate a larger dataset,
    -- you can limit the timespan to the current time with the following line
    -- where ordered_at <= {{ var('truncate_timespan_to') }}
),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- text
        name as location_name,

        ---------- numerics
        tax_rate,

        ---------- timestamps
        opened_at as location_opened_at

    from source

)

select * from renamed
