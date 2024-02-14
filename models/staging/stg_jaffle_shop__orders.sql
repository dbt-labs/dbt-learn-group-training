with

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

transformed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status,
        _etl_loaded_at

    from source

)

select * from transformed