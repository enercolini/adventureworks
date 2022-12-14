with sales_order_detail_source as(
    select *
    from {{ source('raw_postgres', 'sales_salesorderdetail') }}
)

, sales_order_detail as(
    select
        salesorderid as sales_order_id
        , salesorderdetailid as sales_order_detail_id
        , productid as product_id
        , orderqty as order_quantity
        , unitprice as order_unit_price
        , unitpricediscount as order_unit_price_discount
        , cast(modifieddate as date) as update_date
    from sales_order_detail_source
)

select *
from sales_order_detail
