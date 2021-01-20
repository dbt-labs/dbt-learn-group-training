{% set payment_methods = dbt_utils.get_column_values(table=ref('stg_payments'), column='payment_method']) %}

with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (
    select
        order_id,

        {% for payment_method in payment_methods}

        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
        
        {% if not loop.last %}
        ,
        {% endif %}
        
        {% endfor %}
        -- how to handle trailing comma? (if we remove the last column)

    from payments

    group by 1

)