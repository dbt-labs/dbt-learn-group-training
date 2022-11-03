with orders as (
    select * from {{ ref('stg_orders') }}
),


grouped_payments as(
    select
        order_id,
        sum(amount) as total_amount
    from {{ ref('stg_payments') }}
    where status = 'success'
    group by order_id
)

select 
    orders.order_id,
    grouped_payments.total_amount
from orders left join grouped_payments using (order_id)