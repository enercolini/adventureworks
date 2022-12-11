with address_source as(
    select *
    from {{ source('raw_postgres', 'person_address') }}
)

, address as(    
    select
        addressid as address_id
        , city
        , stateprovinceid as state_province_id
        , cast(modifieddate as date) as updated_date 
    from address_source
)

select *
from address