with daily_metrics as (
    select
        session_date as metric_date,
        data_source as ds,
        case when is_prompt_session then 'prompt' else 'non_prompt' end as st,

        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where meets_scr3) as sessions_with_scr3,
        count(*) filter (where is_no_value_session) as no_value_sessions,
        count(*) filter (where has_share and has_post_share_interaction) as sessions_with_pir,
        count(*) filter (where has_native_session_id) as sessions_with_attribution,

        -- Density
        avg(save_count) filter (where has_save) as avg_saves_per_saving_session,
        percentile_cont(0.5) within group (order by save_count) filter (where has_save) as median_saves_per_saving_session,

        -- Timing
        percentile_cont(0.5) within group (order by time_to_first_save_seconds) filter (where time_to_first_save_seconds is not null) as median_ttfs,
        percentile_cont(0.5) within group (order by time_to_first_share_seconds) filter (where time_to_first_share_seconds is not null) as median_tts

    from {{ ref('fct_session_outcomes') }}
    where session_date is not null
    group by session_date, data_source, case when is_prompt_session then 'prompt' else 'non_prompt' end
),

-- Generate rows for all combinations of data_source and session_type
expanded as (
    -- Per data_source x session_type
    select * from daily_metrics

    union all

    -- All data sources combined, per session_type
    select
        metric_date, 'all' as ds, st,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict), sum(sessions_with_scr3),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, st

    union all

    -- All session types combined, per data_source
    select
        metric_date, ds, 'all' as st,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict), sum(sessions_with_scr3),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
        avg(avg_saves_per_saving_session), avg(median_saves_per_saving_session),
        avg(median_ttfs)::numeric, avg(median_tts)::numeric
    from daily_metrics
    group by metric_date, ds

    union all

    -- All combined
    select
        metric_date, 'all' as ds, 'all' as st,
        sum(total_sessions), sum(sessions_with_save), sum(sessions_with_share),
        sum(sessions_with_psr_broad), sum(sessions_with_psr_strict), sum(sessions_with_scr3),
        sum(no_value_sessions), sum(sessions_with_pir), sum(sessions_with_attribution),
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
        total_sessions,
        sessions_with_save,
        sessions_with_share,
        sessions_with_psr_broad,
        sessions_with_psr_strict,
        sessions_with_scr3,
        no_value_sessions,

        -- Rates
        case when total_sessions > 0 then round(sessions_with_save::numeric / total_sessions, 4) else 0 end as ssr,
        case when total_sessions > 0 then round(sessions_with_share::numeric / total_sessions, 4) else 0 end as shr,
        case when sessions_with_share > 0 then round(sessions_with_pir::numeric / sessions_with_share, 4) else 0 end as pir,
        case when total_sessions > 0 then round(sessions_with_psr_broad::numeric / total_sessions, 4) else 0 end as psr_broad,
        case when total_sessions > 0 then round(sessions_with_psr_strict::numeric / total_sessions, 4) else 0 end as psr_strict,
        case when total_sessions > 0 then round(sessions_with_scr3::numeric / total_sessions, 4) else 0 end as scr3_rate,
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
        -- 7-day rolling averages
        avg(ssr) over (partition by data_source, session_type order by metric_date rows between 6 preceding and current row) as ssr_7d_avg,
        avg(shr) over (partition by data_source, session_type order by metric_date rows between 6 preceding and current row) as shr_7d_avg,
        avg(psr_broad) over (partition by data_source, session_type order by metric_date rows between 6 preceding and current row) as psr_broad_7d_avg,

        -- WoW changes
        ssr - lag(ssr, 7) over (partition by data_source, session_type order by metric_date) as ssr_wow_change,
        psr_broad - lag(psr_broad, 7) over (partition by data_source, session_type order by metric_date) as psr_broad_wow_change
    from with_rates
)

select * from with_rolling
order by metric_date desc, data_source, session_type
