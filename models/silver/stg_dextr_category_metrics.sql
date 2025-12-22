

{{ config(
  enabled=true | false
) }}
with daily_totals as (
    select
        date_trunc('day', query_timestamp)::date as day,
        sum(adventure_count)     as adventure_count,
        sum(culture_count)       as culture_count,
        sum(dining_count)        as dining_count,
        sum(entertainment_count) as entertainment_count,
        sum(health_count)        as health_count,
        sum(drinks_count)        as drinks_count
    from {{ ref("stg_dextr_category") }}
    group by day
),

category_rows as (
    select day, 'Adventure'     as category, adventure_count     as count from daily_totals
    union all
    select day, 'Culture',         culture_count                from daily_totals
    union all
    select day, 'Dining',          dining_count                 from daily_totals
    union all
    select day, 'Entertainment',   entertainment_count          from daily_totals
    union all
    select day, 'Health',          health_count                 from daily_totals
    union all
    select day, 'Drinks',          drinks_count                 from daily_totals
),

ranked as (
    select
        day,
        category,
        count,
        row_number() over (partition by day order by count desc) as rn
    from category_rows
),

top_category_per_day as (
    select
        day,
        category as top_category,
        count    as top_category_count
    from ranked
    where rn = 1
)

select
    d.day,
    t.top_category,
    t.top_category_count,

    d.adventure_count,
    d.culture_count,
    d.dining_count,
    d.entertainment_count,
    d.health_count,
    d.drinks_count

from daily_totals d
left join top_category_per_day t using (day)
order by d.day
