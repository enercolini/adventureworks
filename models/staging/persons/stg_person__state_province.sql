with state_province_source as(
    select *
    from {{ source('raw_postgres', 'person_stateprovince') }}
)

, state_province as(
    select
        stateprovinceid as state_province_id
        , territoryid as territory_id
        , countryregioncode as country_region_code
        , name as state
        , cast(modifieddate as date) as update_date
    from state_province_source
)

select *
from state_province
