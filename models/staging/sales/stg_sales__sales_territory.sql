with sales_territory_source as(
    select *
    from {{ source('raw_postgres', 'sales_salesterritory') }}
)

, sales_territory as(
    select
        territoryid as territory_id
        , name as country
        , countryregioncode as country_code
        , cast(modifieddate as date) as update_date
    from sales_territory_source
)

select *
from sales_territory
