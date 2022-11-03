select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- `amount` is currently stored in cents, so we convert it to dollars
    {{- cents_to_dollars('amount') -}} as amount

from {{ source('stripe', 'payment')}}