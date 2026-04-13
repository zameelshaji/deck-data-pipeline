{{ config(materialized='table', tags=['experiment', 'ab_test', 'dashboard']) }}

-- =============================================================================
-- Experiment Dashboard Visualization Table
-- Pre-computed time series + rolling averages + cumulative totals by arm
-- Powers the Streamlit A/B experiment dashboard page
-- =============================================================================

with daily as (
    select * from {{ ref('fct_experiment_results') }}
),

-- Fill date gaps so rolling averages don't skip days
date_spine as (
    select generate_series(
        (select min(metric_date) from daily),
        (select max(metric_date) from daily),
        interval '1 day'
    )::date as metric_date
),

arms as (
    select distinct experiment_arm from daily
),

-- Cross join to get every date x arm combination
date_arm_grid as (
    select ds.metric_date, a.experiment_arm
    from date_spine ds
    cross join arms a
),

-- Join daily metrics onto the grid (null where no data for that day)
filled as (
    select
        dag.metric_date,
        dag.experiment_arm,
        coalesce(d.packs_created, 0) as packs_created,
        coalesce(d.unique_users, 0) as unique_users,
        coalesce(d.total_swipes, 0) as total_swipes,
        coalesce(d.total_likes, 0) as total_likes,
        d.avg_like_rate,
        d.avg_save_rate,
        d.avg_completion_rate,
        d.psr_broad_rate,
        d.avg_swipe_duration_ms,
        d.avg_refinement_latency_ms,
        coalesce(d.total_refined_batches, 0) as total_refined_batches
    from date_arm_grid dag
    left join daily d
        on dag.metric_date = d.metric_date
        and dag.experiment_arm = d.experiment_arm
)

select
    f.*,

    -- 7-day rolling averages
    avg(f.avg_like_rate) over (
        partition by f.experiment_arm order by f.metric_date
        rows between 6 preceding and current row
    ) as like_rate_7d_avg,

    avg(f.avg_save_rate) over (
        partition by f.experiment_arm order by f.metric_date
        rows between 6 preceding and current row
    ) as save_rate_7d_avg,

    avg(f.psr_broad_rate) over (
        partition by f.experiment_arm order by f.metric_date
        rows between 6 preceding and current row
    ) as psr_broad_7d_avg,

    -- Cumulative totals
    sum(f.packs_created) over (
        partition by f.experiment_arm order by f.metric_date
    ) as cumulative_packs,

    sum(f.total_swipes) over (
        partition by f.experiment_arm order by f.metric_date
    ) as cumulative_swipes,

    sum(f.total_likes) over (
        partition by f.experiment_arm order by f.metric_date
    ) as cumulative_likes

from filled f
order by f.metric_date, f.experiment_arm
