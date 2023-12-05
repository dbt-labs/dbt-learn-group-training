with orders as (
    select * from {{ ref('int_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

joined as (
    select 
        order_items.*,
        orders.* exclude (order_id)
        
    from orders 
        inner join order_items 
            on orders.order_id = order_items.order_id
)

select * from joined
