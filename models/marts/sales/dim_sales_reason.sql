with sales_order_header_sales_reason_cte as (
    select
        sales_order_id
        , sales_reason_id
        , update_date
    from {{ ref('stg_erp__sales_order_header_sales_reason') }}
)

, sales_reason_cte as (
    select     
        sales_reason_id
        , sale_reason
    from {{ ref('stg_erp__sales_reason') }}
)

, joined_tables as(
    select
        row_number() over (partition by sales_order_header_sales_reason_cte.sales_order_id order by sales_order_header_sales_reason_cte.update_date desc) as sales_reason_deduplicate
        , sales_order_id
        , sales_order_header_sales_reason_cte.sales_reason_id
        , sales_reason_cte.sale_reason
    from sales_order_header_sales_reason_cte
    left join sales_reason_cte on sales_order_header_sales_reason_cte.sales_reason_id = sales_reason_cte.sales_reason_id
)

, table_deduplicated as(
    select
        row_number() over(order by sales_order_id) as sales_reason_sk
        , sales_order_id
        , sales_reason_id
        , sale_reason
    from joined_tables
    where sales_reason_deduplicate = 1
)

select *
from table_deduplicated