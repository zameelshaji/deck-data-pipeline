-- Deep dive on save patterns and sources
-- Tracks deduplication and source breakdown

with weekly_saves as (
    select
        save_week,
        save_source,
        original_source,

        -- Counts
        count(*) as save_count,
        count(distinct user_id) as unique_savers,
        count(distinct card_id) as unique_cards_saved,

        -- Deduplication metrics
        count(case when is_deduplicated then 1 end) as saves_that_were_deduplicated,
        count(case when dedup_window_match then 1 end) as saves_with_match_in_other_source

    from {{ ref('int_user_saves_unified') }}
    group by save_week, save_source, original_source
),

weekly_totals as (
    select
        save_week,
        sum(save_count) as total_saves,
        count(distinct user_id) as total_unique_savers
    from {{ ref('int_user_saves_unified') }}
    group by save_week
),

-- Users who saved via both sources
dual_source_users as (
    select
        save_week,
        count(distinct user_id) as users_both_sources
    from (
        select
            user_id,
            save_week,
            count(distinct original_source) as source_count
        from {{ ref('int_user_saves_unified') }}
        group by user_id, save_week
        having count(distinct original_source) > 1
    ) sub
    group by save_week
)

select
    ws.save_week,
    ws.save_source,
    ws.original_source,

    -- Volume
    ws.save_count,
    ws.unique_savers,
    ws.unique_cards_saved,

    -- Percentage of total
    round(100.0 * ws.save_count / nullif(wt.total_saves, 0), 2) as pct_of_total_saves,

    -- Saves per user
    round(ws.save_count::numeric / nullif(ws.unique_savers, 0), 2) as saves_per_user,

    -- Card diversity (unique cards / total saves)
    round(100.0 * ws.unique_cards_saved / nullif(ws.save_count, 0), 2) as card_diversity_pct,

    -- Deduplication metrics
    ws.saves_that_were_deduplicated,
    round(100.0 * ws.saves_that_were_deduplicated / nullif(ws.save_count, 0), 2) as pct_deduplicated,
    ws.saves_with_match_in_other_source,

    -- Week totals for context
    wt.total_saves as week_total_saves,
    wt.total_unique_savers as week_unique_savers,
    coalesce(dsu.users_both_sources, 0) as users_using_both_sources

from weekly_saves ws
inner join weekly_totals wt on ws.save_week = wt.save_week
left join dual_source_users dsu on ws.save_week = dsu.save_week
order by ws.save_week desc, ws.save_count desc
