with

source as (
    
    select * from {{ source('jaffle_shop', 'orders') }}
    
),

transformed as (
        
      select 
      
        id as order_id,
        user_id as customer_id,
        status as order_status,
        order_date,
        
        case 
            when order_status not in ('returned','return_pending') 
            then order_date 
        end as valid_order_date,
        
        row_number() over (
            partition by user_id 
            order by order_date, id
        ) as user_order_seq
        
      from source
    
)

select * from transformed