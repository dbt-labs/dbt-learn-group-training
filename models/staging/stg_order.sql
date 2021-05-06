with orders as (
select * from {{ ref('stg_orders') }}
),
payments as (
    select 
    order_id,
    sum(amount) as amount from {{ ref('stg_payments') }}
    where stg_payments.payment_status = 'success'
    group by order_id
)
select
    orders.order_id,
    orders.customer_id,
    payments.amount as total_payment_amount
from  orders
left join  payments on payments.order_id = orders.order_id