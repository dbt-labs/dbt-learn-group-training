with

customers as (
    
    select * from {{ ref('stg_jaffle_shop__customers') }}
    
),

orders as (
    
    select * from {{ ref('int_orders') }}
    
),

final as (
    
    select
    
        orders.*,
        
        customers.surname,
        customers.givenname,
        
        -- Customer-level aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as first_order_date,
        
        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as order_count,

        sum(nvl2(orders.valid_order_date, 1, 0)) over(
            partition by orders.customer_id
        ) as non_returned_order_count,

        array_agg(distinct orders.order_id) over(
            partition by orders.customer_id
        ) as order_ids,
        
        sum(nvl2(orders.valid_order_date, orders.order_value_dollars, 0)) over(
            partition by orders.customer_id
        ) as total_lifetime_value
        
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
    
)

select * from final