with sales_store_source as(
    select *
    from {{ source('raw_postgres', 'sales_store') }}
)

, sales_store as(    
    select
        businessentityid as business_entity_id
        , name as store_name
        , salespersonid as sales_person_id
        , cast(modifieddate as date) as updated_date 
    from sales_store_source
)

select *
from sales_store
