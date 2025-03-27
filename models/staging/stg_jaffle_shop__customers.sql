with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

staged as (

    select
        id as user_id,
        first_name,
        last_name
    from source

)

select * from staged