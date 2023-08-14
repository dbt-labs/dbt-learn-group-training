with

source as (

    select * from raw.jaffle_shop.orders

),

staged as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status,
        _etl_loaded_at

    from source

)

select * from staged