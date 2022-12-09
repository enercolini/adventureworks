with customer_cte as (
    select
        customer_id
        , person_id
    from {{ ref('stg_sales__customer') }}
    where person_id is not null
)

, person_cte as (
    select
        business_entity_id
        , case
            when middle_name is null then concat(first_name, ' ', last_name)
            else concat(first_name, ' ', middle_name,'.', ' ', last_name)
        end as customer_name
    from {{ ref('stg_person__person') }}
)

, person_business_entity_cte as (
    select
       business_entity_id
    from {{ ref('stg_person__business_entity') }}
)

, person_bussiness_entity_address_cte as(
    select
        business_entity_id
        , address_id
    from {{ ref('stg_person__business_entity_address') }}
)

, address_cte as(
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
        customer_cte.customer_id
        , person_cte.customer_name
        , address_cte.city
        , state_province_cte.state
        , country_region_cte.country
    from customer_cte
    left join person_cte on customer_cte.person_id = person_cte.business_entity_id
    left join person_business_entity_cte on person_cte.business_entity_id = person_business_entity_cte.business_entity_id
    left join person_bussiness_entity_address_cte on person_business_entity_cte.business_entity_id = person_bussiness_entity_address_cte.business_entity_id
    left join address_cte on person_bussiness_entity_address_cte.address_id = address_cte.address_id
    left join state_province_cte on address_cte.state_province_id = state_province_cte.state_province_id
    left join country_region_cte on state_province_cte.country_region_code = country_region_cte.country_region_code
)

, table_with_sk as (
    select
    row_number() over (order by customer_id) as customer_sk
    , customer_id
    , customer_name
    , city
    , state
    , country
from joined_tables
)

select *
from table_with_sk

