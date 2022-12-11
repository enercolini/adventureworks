with products_source as(
    select *
    from {{ source('raw_postgres', 'production_product') }}
)

, products as(
    select
        productid as product_id
        , name as product_name
        , standardcost as product_std_cost
        , listprice as product_list_price
        , cast(sellstartdate as date) as product_sell_start_date
        , cast(sellenddate as date) as product_sell_end_date
        , cast(modifieddate as date) as update_date
    from products_source
)

select *
from products
