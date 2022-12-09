with business_entity_source as(
    select *
    from {{ source('raw_postgres', 'person_businessentity') }}
)

, business_entity as(    
    select
        businessentityid as business_entity_id
        , cast(modifieddate as date) as updated_date 
    from business_entity_source
)

select *
from business_entity