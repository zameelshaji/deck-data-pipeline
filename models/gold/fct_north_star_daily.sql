with daily_metrics as (
    select
        session_date as metric_date,
        data_source as ds,
        case when is_prompt_session then 'prompt' else 'non_prompt' end as st,
        coalesce(effective_app_version, 'unknown') as av,

        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where is_no_value_session) as no_value_sessions,
        count(*) filter (where has_share and has_post_share_interaction) as sessions_with_pir,
        count(*) filter (where has_native_session_id) as sessions_with_attribution,
        count(*) filter (where is_genuine_planning_attempt) as genuine_planning_sessions,

        -- Density
        avg(save_count) filter (where has_save) as avg_saves_per_saving_session,
        percentile_cont(0.5) within group (order by save_count) as median_saves_per_saving_session,

        -- Timing
        percentile_cont(0.5) within group (order by time_to_first_save_seconds) as median_ttfs,
        percentile_cont(0.5) within group (order by time_to_first_share_seconds) as median_tts

    from {{ ref('fct_session_outcomes') }}
    where session_date is not null
    group by session_date, data_source, case when is_prompt_session then 'prompt' else 'non_prompt' end, coalesce(effective_app_version, 'unknown')
),

-- Generate all aggregation combinations
expanded as (
    -- Per ds × st × av (base grain)
    select * from daily_metrics

    union all

    -- All app versions, per ds × st
    select
        metric_date, ds, st, 'all' as av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, ds, st

    union all

    -- All data sources, per st × av
    select
        metric_date, 'all' as ds, st, av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, st, av

    union all

    -- All session types, per ds × av
    select
        metric_date, ds, 'all' as st, av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, ds, av

    union all

    -- All ds + all st, per av
    select
        metric_date, 'all' as ds, 'all' as st, av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, av

    union all

    -- All ds + all av, per st
    select
        metric_date, 'all' as ds, st, 'all' as av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, st

    union all

    -- All st + all av, per ds
    select
        metric_date, ds, 'all' as st, 'all' as av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, ds

    union all

    -- All combined
    select
        metric_date, 'all' as ds, 'all' as st, 'all' as av,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        sum(genuine_planning_sessions),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date
),

with_rates as (
    select
        metric_date,
        ds as data_source,
        st as session_type,
        av as app_version,
        total_sessions,
        sessions_with_save,
        sessions_with_share,
        sessions_with_psr_broad,
        sessions_with_psr_strict,
        no_value_sessions,
        genuine_planning_sessions,

        -- Rates
        case when total_sessions > 0 then round(sessions_with_save::numeric / total_sessions, 4) else 0 end as ssr,
        case when total_sessions > 0 then round(sessions_with_share::numeric / total_sessions, 4) else 0 end as shr,
        case when sessions_with_share > 0 then round(sessions_with_pir::numeric / sessions_with_share, 4) else 0 end as pir,
        case when total_sessions > 0 then round(sessions_with_psr_broad::numeric / total_sessions, 4) else 0 end as psr_broad,
        case when total_sessions > 0 then round(sessions_with_psr_strict::numeric / total_sessions, 4) else 0 end as psr_strict,
        case when total_sessions > 0 then round(no_value_sessions::numeric / total_sessions, 4) else 0 end as nvr,
        case when sessions_with_share > 0 then round(sessions_with_pir::numeric / sessions_with_share, 4) else 0 end as validation_rate,
        case when total_sessions > 0 then round(sessions_with_attribution::numeric / total_sessions, 4) else 0 end as attribution_rate,

        round(avg_saves_per_saving_session::numeric, 2) as avg_saves_per_saving_session,
        round(median_saves_per_saving_session::numeric, 2) as median_saves_per_saving_session,
        median_ttfs::integer as median_ttfs,
        median_tts::integer as median_tts

    from expanded
),

with_rolling as (
    select
        *,
        avg(ssr) over (partition by data_source, session_type, app_version order by metric_date rows between 6 preceding and current row) as ssr_7d_avg,
        avg(shr) over (partition by data_source, session_type, app_version order by metric_date rows between 6 preceding and current row) as shr_7d_avg,
        avg(psr_broad) over (partition by data_source, session_type, app_version order by metric_date rows between 6 preceding and current row) as psr_broad_7d_avg,

        ssr - lag(ssr, 7) over (partition by data_source, session_type, app_version order by metric_date) as ssr_wow_change,
        psr_broad - lag(psr_broad, 7) over (partition by data_source, session_type, app_version order by metric_date) as psr_broad_wow_change
    from with_rates
)

select * from with_rolling
order by metric_date desc, data_source, session_type, app_version
