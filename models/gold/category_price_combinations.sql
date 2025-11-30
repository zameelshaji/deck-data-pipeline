{{ config(materialized='table') }}

with events as (
    select
        card_id::text as card_id,
        intent_stage
    from {{ ref('stg_events_intent') }}
    where card_id is not null
      and intent_stage in ('Interest', 'Conversion')
),

cards as (
    select
        card_id::text as card_id,

        -- primary category from flags
        case 
            when is_adventure     then 'Adventure'
            when is_culture       then 'Culture'
            when is_dining        then 'Dining'
            when is_entertainment then 'Entertainment'
            when is_health        then 'Health'
            when is_drinks        then 'Drinks'
        end as category,

        case
            when price_level is null then 'No Budget'
            when price_level <= 1    then 'Budget'
            when price_level = 2     then 'Mid'
            when price_level >= 3    then 'Premium'
        end as price_band
    from {{ ref('stg_cards') }}
),

joined as (
    select
        c.category,
        c.price_band,
        e.intent_stage
    from events e
    join cards c
        on e.card_id = c.card_id
    where c.category is not null
      and c.price_band is not null
),

agg as (
    select
        category,
        price_band,
        sum(case when intent_stage = 'Interest'   then 1 else 0 end) as interest_events,
        sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_events
    from joined
    group by category, price_band        
)

select
    category, price_band,
    interest_events,
    conversion_events
from agg
order by category, price_band