with sales_order_header_source as(
    select *
    from {{ source('raw_postgres', 'sales_salesorderheader') }}
)

, sales_order_header as(
    select
        salesorderid as sales_order_id
        , cast(orderdate as date) as order_date
        , cast(duedate as date) as order_due_date
        , cast(shipdate as date) as order_ship_date
        , status as order_status
        , customerid as customer_id
        , territoryid as territory_id
        , creditcardid as credit_card_id
        , subtotal as order_subtotal
        , taxamt as order_tax_amount
        , freight as order_freight
        , totaldue as order_income
        , cast(modifieddate as date) as update_date 
    from sales_order_header_source
)

select *
from sales_order_header
