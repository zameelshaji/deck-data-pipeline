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

cumulative_totals as (
    select
        day,

        sum(adventure_count)     over (order by day rows between unbounded preceding and current row) as cumulative_adventure,
        sum(culture_count)       over (order by day rows between unbounded preceding and current row) as cumulative_culture,
        sum(dining_count)        over (order by day rows between unbounded preceding and current row) as cumulative_dining,
        sum(entertainment_count) over (order by day rows between unbounded preceding and current row) as cumulative_entertainment,
        sum(health_count)        over (order by day rows between unbounded preceding and current row) as cumulative_health,
        sum(drinks_count)        over (order by day rows between unbounded preceding and current row) as cumulative_drinks

    from daily_totals
)

select *
from cumulative_totals
order by day desc
