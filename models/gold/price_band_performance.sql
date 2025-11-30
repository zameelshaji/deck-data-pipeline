{{ config(materialized='table') }}

with events as (
    select
        date(event_timestamp)::date as day,
        card_id::text as card_id,
        intent_stage
    from {{ ref('stg_events_intent') }}
    where card_id is not null
      and intent_stage in ('Interest', 'Conversion')
),

cards as (
    select
        card_id::text as card_id,
        price_level,
        case
            when price_level is null then 'Zero'
            when price_level <= 1 then 'Budget'
            when price_level = 2 then 'Mid'
            when price_level = 3 then 'Premium'
            else 'Luxury'
        end as price_band
    from {{ ref('stg_cards') }}
),

joined as (
    select
        e.day,
        c.price_band,
        e.intent_stage
    from events e
    join cards c
        on e.card_id = c.card_id
),

agg as (
    select
        day,
        price_band,
        sum(case when intent_stage = 'Interest'   then 1 else 0 end) as interest_events,
        sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_events
    from joined
    group by day, price_band
)

select
    day,

    -- Zero
    coalesce(max(case when price_band = 'Zero'    then interest_events   end), 0) as zero_interest,
    coalesce(max(case when price_band = 'Zero'    then conversion_events end), 0) as zero_conversion,

    -- Budget
    coalesce(max(case when price_band = 'Budget'  then interest_events   end), 0) as budget_interest,
    coalesce(max(case when price_band = 'Budget'  then conversion_events end), 0) as budget_conversion,

    -- Mid
    coalesce(max(case when price_band = 'Mid'     then interest_events   end), 0) as mid_interest,
    coalesce(max(case when price_band = 'Mid'     then conversion_events end), 0) as mid_conversion,

    -- Premium
    coalesce(max(case when price_band = 'Premium' then interest_events   end), 0) as premium_interest,
    coalesce(max(case when price_band = 'Premium' then conversion_events end), 0) as premium_conversion,

    -- Luxury
    coalesce(max(case when price_band = 'Luxury'  then interest_events   end), 0) as luxury_interest,
    coalesce(max(case when price_band = 'Luxury'  then conversion_events end), 0) as luxury_conversion

from agg
group by day
order by day desc
