with credit_card_source as(
    select *
    from {{ source('raw_postgres', 'sales_creditcard') }}
)

, credit_card as(
    select
        creditcardid as credit_card_id
        , cardtype as credit_card_type
        , cast(modifieddate as date) as updated_date
    from credit_card_source
)

select *
from credit_card
