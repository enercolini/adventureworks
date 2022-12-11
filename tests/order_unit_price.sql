with order_unit_price_test as(
    select
        order_unit_price
    from {{ ref('fact_sales') }}
)

select *
from order_unit_price_test
where order_unit_price < 0