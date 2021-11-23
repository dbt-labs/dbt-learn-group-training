WITH base AS (

	SELECT *
	FROM {{ ref('snowflake_warehouse_metering') }}

), contract_rates AS (

    SELECT *
    FROM {{ ref('snowflake_amortized_rates') }}

), usage AS (

    SELECT 
      warehouse_id,
      warehouse_name,

      start_time,
      end_time,
      DATE_TRUNC('month', end_time)::DATE          AS usage_month,
      DATE_TRUNC('day', end_time)::DATE            AS usage_day,
      DATEDIFF(hour, start_time, end_time)         AS usage_length,
      contract_rates.rate                          AS credit_rate,
      ROUND(credits_used * contract_rates.rate, 2) AS dollars_spent
    FROM base
    LEFT JOIN contract_rates 
      ON DATE_TRUNC('day', end_time) = contract_rates.date_day

)

SELECT *
FROM usage
