with customer_source as(
    select *
    from {{ source('raw_postgres', 'sales_customer') }}
)

, customer as(
    select 
        customerid as customer_id
        , personid as person_id
        , territoryid as territory_id
        , storeid as store_id
        , cast(modifieddate as date) as updated_date 
    from customer_source
)

select *
from customer
