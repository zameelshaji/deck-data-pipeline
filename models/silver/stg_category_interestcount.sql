{{ config(materialized='table') }}

-- likes + saves
with events as (
    select
        card_id::text as card_id,
        event_name
    from {{ ref('stg_events_intent') }}
    where event_name in ('swipe_right', 'saved')
),

-- get categories from card
cards as (
    select
        card_id::text as card_id,
        case 
            when is_adventure     then 'Adventure'
            when is_culture       then 'Culture'
            when is_dining        then 'Dining'
            when is_entertainment then 'Entertainment'
            when is_health        then 'Health'
            when is_drinks        then 'Drinks'
        end as category
    from {{ ref('stg_cards') }}
),

-- Join events to categories
joined as (
    select
        c.category,
        e.event_name
    from events e
    join cards c
      on e.card_id = c.card_id
    where c.category is not null
)

-- Aggregate like + save counts per category
select
    category,
    sum(case when event_name = 'swipe_right' then 1 else 0 end) as like_count,
    sum(case when event_name = 'saved'       then 1 else 0 end) as save_count
from joined
group by category
order by like_count desc
