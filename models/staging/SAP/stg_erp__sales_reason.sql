with sales_reason_source as(
    select *
    from {{ source('raw_postgres', 'sales_salesreason') }}
)

, sales_reason as(
    select
        salesreasonid as sales_reason_id
        , name as sale_reason
        , reasontype as sale_category
        , cast(modifieddate as date) as update_date
    from sales_reason_source
)

select *
from sales_reason
