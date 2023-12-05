with products as (
    select * from {{ ref('stg_products') }}
),

supplies as (
    select * from {{ ref('stg_supplies') }}
),

product_supplies_summary as (
    select
        product_id,
        sum(supply_cost) as supply_cost
    from supplies
    group by 1
),

final as (
    select
        products.*,
        product_supplies_summary.supply_cost
    from products
        left join product_supplies_summary 
            on products.product_id = product_supplies_summary.product_id
)

select * from final
