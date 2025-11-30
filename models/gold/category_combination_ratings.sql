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
        trim(both ', ' from concat_ws(', ',
            case when is_adventure     then 'Adventure'     end,
            case when is_culture       then 'Culture'       end,
            case when is_dining        then 'Dining'        end,
            case when is_entertainment then 'Entertainment' end,
            case when is_health        then 'Health'        end,
            case when is_drinks        then 'Drinks'        end
        )) as category_combo,
        case
            when rating is null       then 'Unrated'
            when rating < 3.0         then 'Low (< 3.0)'
            when rating < 4.0         then 'Good (3.0-3.9)'
            when rating < 4.5         then 'Great (4.0-4.4)'
            else 'Excellent (4.5+)'
        end as rating_bucket
    from {{ ref('stg_cards') }}
    where trim(both ', ' from concat_ws(', ',
            case when is_adventure     then 'Adventure'     end,
            case when is_culture       then 'Culture'       end,
            case when is_dining        then 'Dining'        end,
            case when is_entertainment then 'Entertainment' end,
            case when is_health        then 'Health'        end,
            case when is_drinks        then 'Drinks'        end
        )) is not null and trim(both ', ' from concat_ws(', ',
            case when is_adventure     then 'Adventure'     end,
            case when is_culture       then 'Culture'       end,
            case when is_dining        then 'Dining'        end,
            case when is_entertainment then 'Entertainment' end,
            case when is_health        then 'Health'        end,
            case when is_drinks        then 'Drinks'        end
        )) <> ''
),

joined as (
    select
        c.category_combo,
        c.rating_bucket,
        e.intent_stage
    from events e
    join cards c on e.card_id = c.card_id
),

agg as (
    select
        category_combo,
        rating_bucket,
        sum(case when intent_stage = 'Interest' then 1 else 0 end) as interest_events,
        sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_events
    from joined
    group by category_combo, rating_bucket
)

select
    category_combo,
    rating_bucket,
    interest_events,
    conversion_events
from agg
order by category_combo, rating_bucket