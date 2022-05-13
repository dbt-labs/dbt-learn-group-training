
-- Use the `ref` function to select from other models
-- Introducing a merge issue!
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
