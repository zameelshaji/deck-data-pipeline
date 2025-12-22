-- User Segmentation Summary View
-- High-level breakdown of Planners vs Passengers for dashboards

with
    segmentation_totals as (
        select
            user_type,
            count(*) as user_count,
            count(case when days_active > 0 then 1 end) as active_users,
            count(case when days_active = 0 then 1 end) as inactive_users,

            -- Engagement metrics
            avg(case when days_active > 0 then prompts_per_active_day end)
                as avg_prompts_per_active_day,
            avg(case when days_active > 0 then swipes_per_prompt end)
                as avg_swipes_per_prompt,
            avg(case when days_active > 0 then conversion_rate end)
                as avg_conversion_rate,

            -- Activity metrics
            avg(days_active) as avg_days_active,
            avg(total_conversions) as avg_conversions,

            -- Retention proxy
            avg(
                case
                    when days_since_signup > 0
                    then 100.0 * days_active / days_since_signup
                end
            ) as avg_activity_rate

        from {{ ref("user_segmentation") }}
        group by user_type
    ),

    criteria_stats as (
        select
            count(
                case
                    when 'prompt_and_swipe' = any (planner_criteria_met) then 1
                end
            ) as planners_via_prompt_swipe,
            count(
                case when 'created_deck' = any (planner_criteria_met) then 1 end
            ) as planners_via_deck,
            count(
                case
                    when 'shared_content' = any (planner_criteria_met) then 1
                end
            ) as planners_via_share,
            count(
                case
                    when 'created_multiplayer' = any (planner_criteria_met)
                    then 1
                end
            ) as planners_via_multiplayer,

            count(
                case when array_length(planner_criteria_met, 1) > 1 then 1 end
            ) as planners_multi_criteria

        from {{ ref("user_segmentation") }}
        where user_type = 'Planner'
    ),

    overall_stats as (
        select
            count(*) as total_users,
            count(case when user_type = 'Planner' then 1 end) as total_planners,
            count(case when user_type = 'Passenger' then 1 end)
                as total_passengers,

            round(
                100.0 * count(case when user_type = 'Planner' then 1 end) / count(*),
                2
            ) as planner_percentage,

            round(
                100.0
                * count(case when user_type = 'Planner' and days_active > 0 then 1 end)
                / count(case when user_type = 'Planner' then 1 end),
                2
            ) as planner_activation_rate,

            round(
                100.0
                * count(
                    case when user_type = 'Passenger' and days_active > 0 then 1 end
                )
                / count(case when user_type = 'Passenger' then 1 end),
                2
            ) as passenger_activation_rate

        from {{ ref("user_segmentation") }}
    )

select
    -- Overall statistics
    os.total_users,
    os.total_planners,
    os.total_passengers,
    os.planner_percentage,
    os.planner_activation_rate,
    os.passenger_activation_rate,

    -- Planner engagement metrics
    round(st_p.avg_prompts_per_active_day, 2)
        as planner_avg_prompts_per_active_day,
    round(st_p.avg_swipes_per_prompt, 2) as planner_avg_swipes_per_prompt,
    round(st_p.avg_conversion_rate, 2) as planner_avg_conversion_rate,
    round(st_p.avg_days_active, 1) as planner_avg_days_active,
    round(st_p.avg_activity_rate, 2) as planner_avg_activity_rate,

    -- Passenger engagement metrics
    round(st_ps.avg_prompts_per_active_day, 2)
        as passenger_avg_prompts_per_active_day,
    round(st_ps.avg_swipes_per_prompt, 2) as passenger_avg_swipes_per_prompt,
    round(st_ps.avg_conversion_rate, 2) as passenger_avg_conversion_rate,
    round(st_ps.avg_days_active, 1) as passenger_avg_days_active,
    round(st_ps.avg_activity_rate, 2) as passenger_avg_activity_rate,

    -- Planner criteria breakdown
    cs.planners_via_prompt_swipe,
    cs.planners_via_deck,
    cs.planners_via_share,
    cs.planners_via_multiplayer,
    cs.planners_multi_criteria,

    -- Engagement quality comparison
    round(
        st_p.avg_prompts_per_active_day / nullif(st_ps.avg_prompts_per_active_day, 0),
        2
    ) as planner_vs_passenger_prompt_ratio,
    round(
        st_p.avg_conversion_rate / nullif(st_ps.avg_conversion_rate, 0), 2
    ) as planner_vs_passenger_conversion_ratio,

    -- Current timestamp
    current_timestamp as report_generated_at

from overall_stats os
cross join segmentation_totals st_p
cross join segmentation_totals st_ps
cross join criteria_stats cs
where
    st_p.user_type = 'Planner'
    and st_ps.user_type = 'Passenger'
