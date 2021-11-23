SELECT usage_month,
        sum(dollars_spent) AS spend
FROM {{ref('snowflake_warehouse_metering_xf')}}
WHERE [usage_month=daterange]
  AND usage_month < date_trunc('month', CURRENT_TIMESTAMP)::DATE
  AND [warehouse_name=warehouse_name]
GROUP BY 1
ORDER BY 1 DESC
