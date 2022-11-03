-- select * from {{ ref('stg_payments') }}

-- select 
-- 	order_id,
-- 	sum(amount)
-- from {{ ref('stg_payments') }}
-- groupby 1

-- select 
-- 	order_id,
-- 	sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount
-- from {{ ref('stg_payments') }}
-- groupby 1

-- select 
--     order_id,

--     {% for payment_method in ['credit_card', 'bank_transfer', 'gift_card', 'coupon'] %}
--     sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
--     {% endfor %}
--     sum(amount) as total_amount

-- from {{ ref('stg_payments') }}
-- group by order_id