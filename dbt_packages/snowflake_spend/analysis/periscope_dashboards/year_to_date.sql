SELECT sum(dollars_spent) AS dollars_spent
FROM {{ref('snowflake_warehouse_metering_xf')}}
WHERE date_trunc('year', usage_month) = date_trunc('year', CURRENT_TIMESTAMP)::date
