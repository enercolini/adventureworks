with country_region_source as(
    select *
    from {{ source('raw_postgres', 'person_countryregion') }}
)

, country_region as(    
    select
        countryregioncode as country_region_code
        , name as country
        , cast(modifieddate as date) as updated_date 
    from country_region_source
)

select *
from country_region