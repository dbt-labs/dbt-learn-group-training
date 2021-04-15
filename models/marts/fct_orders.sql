with

customers as (
    
    select * from {{ ref('stg_jaffle_shop__customers') }}
    
),

orders as (
    
    select * from {{ ref('stg_jaffle_shop__orders') }}
    
),

payments as (
    
    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
    
),
---------------------------------- Marts
customer_order_history as (
    
    select 

        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,

        min(orders.order_date) as first_order_date,

        min(case 
            when orders.order_status not in ('returned','return_pending') 
            then orders.order_date 
        end) as first_non_returned_order_date,

        max(case 
            when orders.order_status not in ('returned','return_pending') 
            then orders.order_date 
        end) as most_recent_non_returned_order_date,

        coalesce(max(orders.user_order_seq),0) as order_count,

        coalesce(count(case 
            when orders.order_status != 'returned' 
            then 1 end),
            0
        ) as non_returned_order_count,

        sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payments.payment_amount_usd 
            else 0 
        end) as total_lifetime_value,

        sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payments.payment_amount_usd 
            else 0 
        end)
        / nullif(count(case 
            when orders.order_status not in ('returned','return_pending') 
            then 1 end),
            0
        ) as avg_non_returned_order_value,

        array_agg(distinct orders.order_id) as order_ids

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    left outer join payments
    on orders.order_id = payments.order_id

    

    group by 
        customers.customer_id, 
        customers.full_name, 
        customers.surname, 
        customers.givenname
        
),

final as (
    
    select 

        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        customer_order_history.first_order_date,
        customer_order_history.order_count,
        customer_order_history.total_lifetime_value,
        payments.payment_amount_usd as order_value_dollars,
        orders.order_status,
        payments.payment_status
        
    from orders

    join customers
    on orders.customer_id = customers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id

    left outer join payments
    on orders.order_id = payments.order_id
    
)

select * from final

