with events as (
    select
        date(event_timestamp) as day,
        card_id,
        intent_stage
    from {{ ref('stg_events_intent') }}   -- your events+intent model
    where card_id is not null
),

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
    from {{ ref('stg_cards') }}           -- unified cards model
),

joined as (
    select
        e.day,
        c.category,
        e.intent_stage
    from events e
    left join cards c
        on e.card_id = c.card_id
    where c.category is not null
),

-- daily totals per day + category
daily as (
    select
        day,
        category,
        sum(case when intent_stage = 'Interest'   then 1 else 0 end) as interest_count,
        sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_count
    from joined
    group by day, category
)

select
    day,

    -- Adventure: [interest, conversion]
    array[
        coalesce(max(case when category = 'Adventure' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Adventure' then conversion_count end), 0)::int
    ] as adventure,

    -- Culture
    array[
        coalesce(max(case when category = 'Culture' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Culture' then conversion_count end), 0)::int
    ] as culture,

    -- Dining
    array[
        coalesce(max(case when category = 'Dining' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Dining' then conversion_count end), 0)::int
    ] as dining,

    -- Drinks
    array[
        coalesce(max(case when category = 'Drinks' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Drinks' then conversion_count end), 0)::int
    ] as drinks,

    -- Entertainment
    array[
        coalesce(max(case when category = 'Entertainment' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Entertainment' then conversion_count end), 0)::int
    ] as entertainment,

    -- Health
    array[
        coalesce(max(case when category = 'Health' then interest_count   end), 0)::int,
        coalesce(max(case when category = 'Health' then conversion_count end), 0)::int
    ] as health

from daily
group by day
order by day desc

