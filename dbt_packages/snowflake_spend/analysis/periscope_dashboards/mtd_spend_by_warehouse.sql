SELECT  warehouse_name,
        sum(dollars_spent) as spend
FROM  {{ref('snowflake_warehouse_metering_xf')}}
WHERE usage_month = date_trunc('month', CURRENT_TIMESTAMP)::date
  AND [warehouse_name=warehouse_name]
GROUP BY 1
ORDER BY 1 DESC
