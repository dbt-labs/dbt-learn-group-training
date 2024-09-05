with 

source as (

    select * from {{ source('snowplow', 'events') }}

),

renamed as (

    select
        event_id,
        page_view_id,
        anonymous_user_id,
        session_id,
        event,
        device_type,
        page_url,
        page_title,
        page_urlscheme,
        page_urlhost,
        page_urlport,
        page_urlpath,
        page_urlquery,
        page_urlfragment,
        collector_tstamp,
        derived_tstamp

    from source

)

select * from renamed
