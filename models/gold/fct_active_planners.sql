with weekly as (
    select
        session_week as period_start,
        session_week + 6 as period_end,
        'week' as period_type,
        count(distinct user_id) filter (where has_save or has_share) as active_planners,
        count(distinct user_id) filter (where has_save) as active_savers,
        count(distinct user_id) filter (where has_share) as active_sharers
    from {{ ref('fct_session_outcomes') }}
    where session_week is not null
    group by session_week
),

monthly as (
    select
        date_trunc('month', session_date)::date as period_start,
        (date_trunc('month', session_date) + interval '1 month' - interval '1 day')::date as period_end,
        'month' as period_type,
        count(distinct user_id) filter (where has_save or has_share) as active_planners,
        count(distinct user_id) filter (where has_save) as active_savers,
        count(distinct user_id) filter (where has_share) as active_sharers
    from {{ ref('fct_session_outcomes') }}
    where session_date is not null
    group by date_trunc('month', session_date)
),

-- Stickiness: avg WAP / MAP for each month
monthly_with_stickiness as (
    select
        m.*,
        (
            select avg(w.active_planners)
            from weekly w
            where w.period_start >= m.period_start
              and w.period_start <= m.period_end
        ) as avg_wap_in_month,
        case
            when m.active_planners > 0 then round(
                (
                    select avg(w.active_planners)
                    from weekly w
                    where w.period_start >= m.period_start
                      and w.period_start <= m.period_end
                )::numeric / m.active_planners, 4
            )
            else 0
        end as planner_stickiness
    from monthly m
)

select
    period_type,
    period_start,
    period_end,
    active_planners,
    active_savers,
    active_sharers,
    null::numeric as avg_wap_in_month,
    null::numeric(5,4) as planner_stickiness
from weekly

union all

select
    period_type,
    period_start,
    period_end,
    active_planners,
    active_savers,
    active_sharers,
    round(avg_wap_in_month::numeric, 0) as avg_wap_in_month,
    planner_stickiness
from monthly_with_stickiness

order by period_start desc, period_type
