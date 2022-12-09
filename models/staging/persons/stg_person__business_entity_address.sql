with business_entity_address_source as(
    select *
    from {{ source('raw_postgres', 'person_businessentityaddress') }}
)

, business_entity_address as(    
    select
        businessentityid as business_entity_id
        , addressid as address_id
        , cast(modifieddate as date) as updated_date 
    from business_entity_address_source
)

select *
from business_entity_address
