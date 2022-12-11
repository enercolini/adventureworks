with sales_order_header_sales_reason_source as(
    select *
    from {{ source('raw_postgres', 'sales_salesorderheadersalesreason') }}
)

, sales_order_header_sales_reason as(
    select
        salesorderid as sales_order_id
        , salesreasonid as sales_reason_id
        , cast(modifieddate as date) as update_date
    from sales_order_header_sales_reason_source
)

select *
from sales_order_header_sales_reason
