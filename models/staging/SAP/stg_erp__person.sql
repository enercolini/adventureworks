with person_source as(
    select *
    from {{ source('raw_postgres', 'person_person') }}
)

, person as(
    select
        businessentityid as business_entity_id
        , persontype as person_type
        , firstname as first_name
        , middlename as middle_name
        , lastname as last_name
        , cast(modifieddate as date) as update_date
    from person_source
)

select *
from person