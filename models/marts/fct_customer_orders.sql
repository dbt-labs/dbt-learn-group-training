with 

-- Import CTEs

customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

),

payments as (

  select * from {{ source('stripe', 'payment') }}

),

-- Logical CTEs

completed_payments as (
    select 
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name
    from orders
    left join completed_payments as p on orders.id = p.order_id
    left join customers as c on orders.user_id = c.id
),

customer_orders as (
    select 
        c.id as customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(orders.id) as number_of_orders
    from customers as c 
    left join orders on orders.user_id = c.id 
    group by 1
),

-- Final CTE

final as (
    select
        p.*,

        -- sales transaction sequence
        row_number() over (order by p.order_id) as transaction_seq,

        -- customer sales sequence
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        
        -- new vs returning customer
        case when c.first_order_date = p.order_placed_at
        then 'new'
        else 'return' end as nvsr,
        
        -- customer lifetime value
        sum(total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
            ) as customer_lifetime_value,

        -- first day of sale
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    order by order_id
)

-- Simple Select Statment

select * from final