with sales_order_header_sales_reason_cte as (
    select
        row_number() over (order by sales_order_id) as sales_reason_sk
        , sales_order_id
        , sales_reason_id
    from {{ ref('stg_sales__sales_order_header_sales_reason') }}
)

, sales_reason_cte as (
    select     
        sales_reason_id
        , sale_reason
    from {{ ref('stg_sales__sales_reason') }}
)

, joined_tables as(
    select
        sales_reason_sk
        , sales_order_id
        , sales_reason_cte.sales_reason_id
        , sales_reason_cte.sale_reason
    from sales_order_header_sales_reason_cte
    left join sales_reason_cte on sales_order_header_sales_reason_cte.sales_reason_id = sales_reason_cte.sales_reason_id
)

select *
from joined_tables
