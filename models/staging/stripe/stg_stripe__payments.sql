select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    {{ cents_to_dollars('amount') }} as amount, -- amount is stored in cents, convert it to dollars
    created as created_at

from {{ source('stripe','payment') }}