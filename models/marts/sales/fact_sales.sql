with customers as (
    select
        customer_sk
        , customer_id
    from {{ ref('dim_customers') }}
)

, products as (
    select
        product_sk
        , product_id
    from {{ ref('dim_products') }}
)

, credit_card as (
    select
        credit_card_sk
        , credit_card_id
    from {{ ref('dim_credit_card') }}
)

, sales_reason as (
    select
        sales_reason_sk
        , sales_order_id
    from {{ ref('dim_sales_reason') }}
)

, sales_order_detail_joined as (
    select
        sales_order_id
        , products.product_sk as product_fk
        , order_quantity
        , order_unit_price
    from {{ ref('stg_sales__sales_order_detail') }} as sales_order_detail
    left join products on sales_order_detail.product_id = products.product_id
)

, sales_order_header_joined as (
    select
        sales_order_header.sales_order_id
        , customer_sk as customer_fk
        , credit_card_sk as credit_card_fk
        , sales_reason_sk as sales_reason_fk
        , case
            when order_status = 1 then 'In process'
            when order_status = 2 then 'Approved'
            when order_status = 3 then 'Backordered'
            when order_status = 4 then 'Rejected'
            when order_status = 5 then 'Shipped'
            else 'Cancelled'
        end as order_status
    from {{ ref('stg_sales__sales_order_header') }} as sales_order_header
    left join customers on sales_order_header.customer_id = customers.customer_id
    left join credit_card on sales_order_header.credit_card_id = credit_card.credit_card_id
    left join sales_reason on sales_order_header.sales_order_id = sales_reason.sales_order_id
)

, sales as (
    select
        row_number() over(order by sales_order_header.sales_order_id) as sales_sk
        , sales_order_header.customer_fk
        , sales_order_header.credit_card_fk
        , sales_order_header.sales_reason_fk
        , sales_order_detail.product_fk
        , sales_order_header.order_status
        , sales_order_detail.order_quantity
        , sales_order_detail.order_unit_price
    from sales_order_header_joined as sales_order_header
    left join sales_order_detail_joined as sales_order_detail on sales_order_header.sales_order_id = sales_order_detail.sales_order_id
)

select *
from sales
