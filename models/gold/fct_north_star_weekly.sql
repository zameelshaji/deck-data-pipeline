with weekly_metrics as (
    select
        session_week as metric_week,
        data_source,
        case when is_prompt_session then 'prompt' else 'non_prompt' end as session_type,
        coalesce(effective_app_version, 'unknown') as app_version,

        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where is_no_value_session) as no_value_sessions,

        -- Unique active planners
        count(distinct user_id) filter (where has_save or has_share) as weekly_active_planners,
        count(distinct user_id) filter (where has_save) as weekly_active_savers,
        count(distinct user_id) filter (where has_share) as weekly_active_sharers,

        -- Density
        avg(save_count) filter (where has_save) as avg_saves_per_saving_session,
        percentile_cont(0.5) within group (order by time_to_first_save_seconds) as median_ttfs,
        percentile_cont(0.5) within group (order by time_to_first_share_seconds) as median_tts

    from {{ ref('fct_session_outcomes') }}
    where session_week is not null
    group by session_week, data_source, case when is_prompt_session then 'prompt' else 'non_prompt' end, coalesce(effective_app_version, 'unknown')
),

-- Recompute "all" aggregation from base data to avoid overcounting distinct users
all_combined as (
    select
        session_week as metric_week,
        'all' as data_source,
        'all' as session_type,
        'all' as app_version,
        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where is_no_value_session) as no_value_sessions,
        count(distinct user_id) filter (where has_save or has_share) as weekly_active_planners,
        count(distinct user_id) filter (where has_save) as weekly_active_savers,
        count(distinct user_id) filter (where has_share) as weekly_active_sharers,
        avg(save_count) filter (where has_save) as avg_saves_per_saving_session,
        percentile_cont(0.5) within group (order by time_to_first_save_seconds) as median_ttfs,
        percentile_cont(0.5) within group (order by time_to_first_share_seconds) as median_tts
    from {{ ref('fct_session_outcomes') }}
    where session_week is not null
    group by session_week
),

-- All app versions combined, per ds × st (recompute for correct distinct counts)
all_av_per_ds_st as (
    select
        session_week as metric_week,
        data_source,
        case when is_prompt_session then 'prompt' else 'non_prompt' end as session_type,
        'all' as app_version,
        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where is_no_value_session) as no_value_sessions,
        count(distinct user_id) filter (where has_save or has_share) as weekly_active_planners,
        count(distinct user_id) filter (where has_save) as weekly_active_savers,
        count(distinct user_id) filter (where has_share) as weekly_active_sharers,
        avg(save_count) filter (where has_save) as avg_saves_per_saving_session,
        percentile_cont(0.5) within group (order by time_to_first_save_seconds) as median_ttfs,
        percentile_cont(0.5) within group (order by time_to_first_share_seconds) as median_tts
    from {{ ref('fct_session_outcomes') }}
    where session_week is not null
    group by session_week, data_source, case when is_prompt_session then 'prompt' else 'non_prompt' end
),

unioned as (
    select * from weekly_metrics
    union all
    select * from all_av_per_ds_st
    union all
    select * from all_combined
),

with_rates as (
    select
        *,
        case when total_sessions > 0 then round(sessions_with_save::numeric / total_sessions, 4) else 0 end as ssr,
        case when total_sessions > 0 then round(sessions_with_share::numeric / total_sessions, 4) else 0 end as shr,
        case when total_sessions > 0 then round(sessions_with_psr_broad::numeric / total_sessions, 4) else 0 end as psr_broad,
        case when total_sessions > 0 then round(sessions_with_psr_strict::numeric / total_sessions, 4) else 0 end as psr_strict,
        case when total_sessions > 0 then round(no_value_sessions::numeric / total_sessions, 4) else 0 end as nvr,

        -- WoW
        lag(sessions_with_save::numeric / nullif(total_sessions, 0), 1) over (partition by data_source, session_type, app_version order by metric_week) as prev_week_ssr,
        lag(weekly_active_planners, 1) over (partition by data_source, session_type, app_version order by metric_week) as prev_week_wap
    from unioned
)

select
    *,
    ssr - coalesce(prev_week_ssr, ssr) as ssr_wow_change,
    weekly_active_planners - coalesce(prev_week_wap, weekly_active_planners) as wap_wow_change
from with_rates
order by metric_week desc, data_source, session_type, app_version
