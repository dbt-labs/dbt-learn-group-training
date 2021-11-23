SELECT usage_month,
        date_part('dayofmonth', end_time) as spend_day,
        sum(dollars_spent) as spend,
        sum(spend) over(partition by usage_month order by spend_day rows between UNBOUNDED PRECEDING and current row) as cumulative
FROM {{ref('snowflake_warehouse_metering_xf')}}
WHERE date_trunc('year', usage_month)::date = date_trunc('year', CURRENT_TIMESTAMP)::date
GROUP BY 1, 2
ORDER BY 1 DESC
