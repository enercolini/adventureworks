with address_cte as(
    select
        address_id
        , state_province_id
        , city
    from {{ ref('stg_person__address') }}
)

, state_province_cte as(
    select
        state_province_id
        , country_region_code
        , state
    from {{ ref('stg_person__state_province') }}
)

, country_region_cte as(
    select
        country_region_code
        , country
    from {{ ref('stg_person__country_region') }}
)

, joined_tables as(
    select
        row_number() over (order by address_cte.address_id) as address_sk
        , address_id
        , city
        , state_province_cte.state
        , country_region_cte.country
    from address_cte
    left join state_province_cte on address_cte.state_province_id =  state_province_cte.state_province_id
    left join country_region_cte on state_province_cte.country_region_code = country_region_cte.country_region_code
)

select *
from joined_tables
