with

application_history as ( select * from {{ ref('application_history') }})
, job_stages as ( select * from {{ ref('job_stages') }}) -- represents the job stages joined with the "official stage" seed

, possible_application_job_stages as ( -- get every possible stage for every application
  
  select distinct

    application_history.application_id
    , job_stages.job_stage_id
    , job_stages.job_stage_name
    , job_stages.job_stage_index
    
  from application_history
  join job_stages

)

, actual_job_stages as ( -- get all actual applications + stages
  
  select  

    application_history.*
    , job_stages.job_stage_name
    , job_stages.job_stage_index

  from application_history
  left join job_stages
    on application_history.job_stage_id = job_stages.job_stage_id

)

, add_synthetic as ( -- join actual data onto all possibilities so we can see all possible+actual
  
  select  

    possible_application_job_stages.*
    , actual_job_stages.status
    , actual_job_stages.created_at

    , case 
      when actual_job_stages.application_id is null 
      then 'sythetic' else 'actual' 
    end as stage_type -- identify if synthetic or not

    , max(actual_job_stages.job_stage_index) over(
        partition by possible_application_job_stages.application_id
    ) as last_stage_completed -- Add the last stage completed for filtering abilities

  from possible_application_job_stages
  left join actual_job_stages
    on possible_application_job_stages.application_id = actual_job_stages.application_id
    and possible_application_job_stages.job_stage_id = actual_job_stages.job_stage_id

)

, fill_and_filter_substages as (
  
  select

    add_synthetic.*

    -- assume the sub-stage belongs to the last "official" stage
    , case 
        when job_stage_index is null -- if null, then it's a sub-stage. Make it null.
        then lag(case when job_stage_index is not null then job_stage_name end) ignore nulls over(
          partition by application_id 
          order by created_at
        ) 
        else job_stage_name
      end as reassigned_job_stage_name

    , case 
        when job_stage_index is null -- if null, then it's a sub-stage. Make it null.
        then lag(case when job_stage_index is not null then job_stage_index end) ignore nulls over(
          partition by application_id 
          order by created_at
        ) 
        else job_stage_index
    end as reassigned_job_stage_index

  from add_synthetic
  where stage_type = 'actual' or job_stage_index <= last_stage_completed -- filter out stages they haven't reached yet

),

substage_context_aggs as ( -- gets the latest status using the reassigned job stage

  select

    *
    , last_value(status) over( -- get the latest status over the "new" job stage categorization
        partition by application_id, reassigned_job_stage_name
        order by created_at
      ) as latest_stage_status

  from fill_and_filter_substages

)

, aggregate_to_stage as ( -- aggregates everything to one row per application+stage

  select

    application_id
    , reassigned_job_stage_name as job_stage_name -- use the new job stage name
    , stage_type
    , any_value(reassigned_job_stage_index) as job_stage_index
    , any_value(latest_stage_status) as latest_status
    , min(created_at) as started_at

  from substage_context_aggs
  group by 1, 2, 3

)

, lags_and_leads as (

  select 

    application_id
    , job_stage_name
    , stage_type
    , job_stage_index
    , latest_status
    , coalesce(started_at, lead(started_at) over(
        partition by application_id 
        order by job_stage_index
      )) as started_at

  from aggregate_to_stage

)

, final as (

    select

      *
      -- The following uses the derived field started_at
      , lead(started_at) over(partition by application_id order by job_stage_index) as ended_at
    
    from lags_and_leads

)

select *
-- from fill_and_filter_stages order by application_id, reassigned_job_stage_name, created_at
-- from fill_with_substage_context order by application_id, created_at, reassigned_job_stage_name
from final order by application_id, job_stage_index
;