with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}
    
    --raw.jaffle_shop.customers

),

transformed as (

    select
        id as customer_id,
        first_name,
        last_name
    from source

)

select * from transformed