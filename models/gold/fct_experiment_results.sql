{{ config(materialized='table', tags=['experiment', 'ab_test']) }}

-- =============================================================================
-- A/B Experiment Results — Daily metrics by experiment arm
-- Grain: one row per (experiment_arm, metric_date)
-- =============================================================================

with experiment_packs as (
    select
        pp.pack_id,
        pp.user_id,
        pp.experiment_arm,
        pp.query_date as metric_date,
        pp.total_cards_generated,
        pp.cards_shown,
        pp.cards_swiped,
        pp.cards_liked,
        pp.cards_disliked,
        pp.cards_saved,
        pp.like_rate,
        pp.save_rate,
        pp.completion_rate,
        pp.candidate_pool_size,
        pp.refined_batch_count,
        pp.avg_refinement_latency_ms,
        pp.led_to_save,
        pp.led_to_share,
        pp.led_to_post_share
    from {{ ref('fct_pack_performance') }} pp
    where pp.experiment_arm is not null
),

-- Swipe-level detail from dextr_places (for swipe_duration_ms)
swipe_stats as (
    select
        dp.pack_id,
        avg(dp.swipe_duration_ms) as avg_swipe_duration_ms,
        count(*) filter (where dp.was_refined = true) as refined_swipe_count
    from {{ ref('src_dextr_places') }} dp
    where dp.user_action in ('like', 'dislike')
    group by dp.pack_id
),

daily_metrics as (
    select
        ep.experiment_arm,
        ep.metric_date,

        -- Volume
        count(distinct ep.pack_id) as packs_created,
        count(distinct ep.user_id) as unique_users,

        -- Swipe metrics
        sum(ep.cards_swiped) as total_swipes,
        sum(ep.cards_liked) as total_likes,
        sum(ep.cards_disliked) as total_dislikes,

        -- Rates (pack-level averages)
        round(avg(ep.like_rate), 4) as avg_like_rate,
        round(avg(ep.save_rate), 4) as avg_save_rate,
        round(avg(ep.completion_rate), 4) as avg_completion_rate,
        sum(ep.cards_saved) as total_saves,

        -- Session outcomes
        round(avg(ep.led_to_save::int::numeric), 4) as sessions_with_save_pct,
        round(avg(ep.led_to_share::int::numeric), 4) as sessions_with_share_pct,
        round(avg(
            case when ep.led_to_save and ep.led_to_share then 1 else 0 end::numeric
        ), 4) as psr_broad_rate,

        -- Refinement
        sum(ep.refined_batch_count) as total_refined_batches,
        round(avg(ep.avg_refinement_latency_ms), 1) as avg_refinement_latency_ms,

        -- Swipe duration
        round(avg(ss.avg_swipe_duration_ms), 0) as avg_swipe_duration_ms

    from experiment_packs ep
    left join swipe_stats ss on ep.pack_id = ss.pack_id
    group by ep.experiment_arm, ep.metric_date
)

select
    dm.*,
    -- Cumulative totals (for significance calculations)
    sum(dm.packs_created) over (
        partition by dm.experiment_arm order by dm.metric_date
    ) as cumulative_packs,
    sum(dm.unique_users) over (
        partition by dm.experiment_arm order by dm.metric_date
    ) as cumulative_user_days,
    sum(dm.total_likes) over (
        partition by dm.experiment_arm order by dm.metric_date
    ) as cumulative_likes,
    sum(dm.total_swipes) over (
        partition by dm.experiment_arm order by dm.metric_date
    ) as cumulative_swipes
from daily_metrics dm
order by dm.metric_date, dm.experiment_arm
