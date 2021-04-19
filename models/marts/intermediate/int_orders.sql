with

orders as (
    
    select * from {{ ref('stg_jaffle_shop__orders') }}
    
),

payments as (
    
    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
    
),

order_totals as (
    
    select
    
        order_id,
        payment_status,
        sum(payment_amount_usd) as order_value_dollars
    
    from payments
    group by 1, 2
    
),

joined as (
    
    select
    
        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars
    
    from orders 
    left join order_totals
        on orders.order_id = order_totals.order_id
    
)

select * from joined

