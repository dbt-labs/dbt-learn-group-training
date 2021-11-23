WITH base AS (

	SELECT *
	FROM {{ source('snowflake','warehouse_metering_history') }}

)

SELECT
  warehouse_id,
  warehouse_name,
  start_time,
  end_time,
  credits_used
FROM base
