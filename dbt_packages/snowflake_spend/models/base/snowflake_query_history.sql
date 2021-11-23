{{
  config(
    materialized='incremental',
    unique_key='snowflake_query_id'
  )
}}


WITH source AS (

  SELECT *
  FROM {{ source('snowflake','query_history') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY query_id) = 1

), renamed AS (

  SELECT 
    query_id                          AS snowflake_query_id,

    -- Foreign Keys
    database_id                       AS database_id,
    schema_id                         AS schema_id,
    session_id                        AS snowflake_session_id,
    warehouse_id                      AS warehouse_id,

    -- Logical Info
    database_name                     AS database_name,
    query_text                        AS query_text,
    role_name                         AS snowflake_role_name,
    rows_produced                     AS query_result_rows_produced,
    schema_name                       AS schema_name,
    user_name                         AS snowflake_user_name,
    warehouse_name                    AS warehouse_name,

    -- metadata 
    end_time                          AS query_end_time,
    start_time                        AS query_start_time,
    bytes_spilled_to_local_storage    AS query_bytes_spillover_local,
    bytes_spilled_to_remote_storage   AS query_bytes_spillover_remote,
    bytes_scanned                     AS query_bytes_scanned

  FROM source
  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE query_start_time > (SELECT MAX(query_start_time)  FROM {{ this }})

  {% endif %}
  
)

SELECT * 
FROM renamed