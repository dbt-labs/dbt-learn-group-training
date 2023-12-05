with

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        location_id as location_id,
        customer_id as customer_id,

        ---------- numerics
        (order_total / 100.0) as order_total,
        (tax_paid / 100.0) as tax_paid,

        ---------- timestamps
        {{ dbt.date_trunc('day','ordered_at') }} as ordered_at

    from source

)

select * from renamed
