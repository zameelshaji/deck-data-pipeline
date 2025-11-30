{{ config(materialized='table') }}

-- 1) Take only events that are tied to a card + have an intent stage
with events as (
    select
        card_id,
        intent_stage
    from {{ ref('stg_events_intent') }}
    where card_id is not null
      and intent_stage in ('Interest', 'Conversion')
),

-- 2) Build a category_list array for each card using your boolean flags
cards as (
    select
        card_id::text as card_id,
        array_remove(
            array[
                case when is_adventure     then 'Adventure'     end,
                case when is_culture       then 'Culture'       end,
                case when is_dining        then 'Dining'        end,
                case when is_entertainment then 'Entertainment' end,
                case when is_health        then 'Health'        end,
                case when is_drinks        then 'Drinks'        end
            ],
            null
        ) as category_list
    from {{ ref('stg_cards') }}
),

-- 3) Join events to cards, so each event knows the category combo of its card
joined as (
    select
        e.intent_stage,
        c.category_list
    from events e
    join cards c
      on e.card_id = c.card_id
    where cardinality(c.category_list) > 0   -- only keep cards that have at least 1 category
),

-- 4) Aggregate by category_list (the combination)
agg as (
    select
        category_list,
        sum(case when intent_stage = 'Interest'   then 1 else 0 end) as interest_events,
        sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_events
    from joined
    group by category_list
)

select
    category_list,          -- e.g. {"Dining","Drinks"}
    interest_events,        -- how many interest actions on cards with this combo
    conversion_events       -- how many conversion actions on cards with this combo

from agg
order by conversion_events desc, interest_events desc
