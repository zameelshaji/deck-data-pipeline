-- Executive summary with key business metrics and health indicators
with current_period_metrics as (
    -- Last 30 days performance
    select 
        'last_30_days' as period,
        
        -- User metrics
        avg(daily_active_users) as avg_daily_users,
        sum(daily_active_users) / count(distinct activity_date) as avg_dau,
        max(daily_active_users) as peak_dau,
        avg(dau_7day_avg) as avg_dau_smoothed,
        
        -- Growth indicators
        avg(dau_wow_growth_percent) as avg_dau_growth,
        avg(ai_adoption_rate) as avg_ai_adoption,
        avg(conversion_user_rate) as avg_conversion_rate,
        
        -- Engagement quality
        avg(avg_events_per_user) as avg_events_per_user,
        avg(multiplayer_active_users) as avg_multiplayer_users
        
    from {{ ref('vis_daily_active_users') }}
    where activity_date >= current_date - interval '30 days'
),

previous_period_metrics as (
    -- Previous 30 days for comparison
    select
        'previous_30_days' as period,
        avg(daily_active_users) as avg_daily_users,
        avg(ai_adoption_rate) as avg_ai_adoption,
        avg(conversion_user_rate) as avg_conversion_rate,
        avg(avg_events_per_user) as avg_events_per_user

    from {{ ref('vis_daily_active_users') }}
    where activity_date between current_date - interval '60 days' and current_date - interval '30 days'
),

acquisition_health as (
    select 
        sum(signups) as total_signups_last_30,
        avg(onboarding_completion_rate) as avg_onboarding_rate,
        avg(ai_adoption_rate) as avg_new_user_ai_adoption,
        avg(week_1_activation_rate) as avg_activation_rate,
        avg(signups_7day_total) as avg_weekly_signups
        
    from {{ ref('vis_user_acquisition_funnel') }}
    where signup_date >= current_date - interval '30 days'
),

ai_performance_health as (
    select 
        avg(total_queries) as avg_daily_queries,
        avg(unique_users) as avg_daily_ai_users,
        avg(avg_like_rate) as avg_ai_satisfaction,
        avg(pack_success_rate) as avg_pack_success,
        avg(avg_processing_time) as avg_response_time,
        avg(power_user_percentage) as ai_power_user_rate
        
    from {{ ref('vis_dextr_performance') }}
    where query_date >= current_date - interval '30 days'
),

content_health as (
    select 
        count(distinct case when metric_type = 'individual_cards' then dimension_value end) as active_cards_count,
        avg(case when metric_type = 'individual_cards' then like_rate end) as avg_content_like_rate,
        avg(case when metric_type = 'individual_cards' then conversion_rate end) as avg_content_conversion_rate,
        avg(case when metric_type = 'individual_cards' then engagement_score end) as avg_engagement_score,
        
        -- Top performing content insights
        count(case when metric_type = 'individual_cards' and engagement_score >= 5 then 1 end) as high_performing_cards,
        count(case when metric_type = 'individual_cards' and like_rate >= 70 then 1 end) as well_liked_cards
        
    from {{ ref('vis_content_performance') }}
    where metric_type = 'individual_cards'
),


multiplayer_health as (
    select 
        count(*) as total_multiplayer_sessions_last_30,
        avg(total_participants) as avg_participants_per_session,
        avg(highest_consensus_percent) as avg_consensus_rate,
        count(case when collaboration_effectiveness in ('Clear Consensus', 'Multiple Strong Options') then 1 end) as successful_collaborations,
        avg(session_duration_hours) as avg_session_duration_hours
        
    from {{ ref('stg_multiplayer') }}
    where created_at >= current_date - interval '30 days'
),

business_health_scores as (
    select 
        -- Overall health score calculation (0-100)
        case 
            when cpm.avg_dau >= 1000 then 25
            when cpm.avg_dau >= 500 then 20
            when cpm.avg_dau >= 100 then 15
            when cpm.avg_dau >= 50 then 10
            else 5
        end +
        case 
            when aph.avg_ai_satisfaction >= 70 then 25
            when aph.avg_ai_satisfaction >= 60 then 20
            when aph.avg_ai_satisfaction >= 50 then 15
            when aph.avg_ai_satisfaction >= 40 then 10
            else 5
        end +
        case 
            when ch.avg_content_like_rate >= 60 then 25
            when ch.avg_content_like_rate >= 50 then 20
            when ch.avg_content_like_rate >= 40 then 15
            when ch.avg_content_like_rate >= 30 then 10
            else 5
        end as overall_health_score,
        
        -- Individual component scores
        case 
            when cpm.avg_dau_growth > 10 then 'Strong Growth'
            when cpm.avg_dau_growth > 0 then 'Positive Growth'
            when cpm.avg_dau_growth > -5 then 'Stable'
            else 'Declining'
        end as growth_health,
        
        case 
            when aph.avg_ai_satisfaction >= 70 then 'Excellent'
            when aph.avg_ai_satisfaction >= 60 then 'Good'
            when aph.avg_ai_satisfaction >= 50 then 'Fair'
            else 'Needs Improvement'
        end as ai_health
        
    from current_period_metrics cpm, ai_performance_health aph, content_health ch
)

-- Final executive summary output
select 
    'executive_summary' as report_type,
    current_date as report_date,
    
    -- Key Performance Indicators
    round(cpm.avg_dau, 0) as daily_active_users,
    round(cpm.avg_dau_growth, 1) as dau_growth_percent,
    round(ah.total_signups_last_30, 0) as new_signups_30d,
    round(ah.avg_activation_rate, 1) as activation_rate,


    
    -- AI Feature Performance
    round(aph.avg_daily_queries, 0) as avg_daily_ai_queries,
    round(aph.avg_ai_satisfaction, 1) as ai_satisfaction_rate,
    round(aph.avg_response_time, 1) as avg_ai_response_time_sec,
    round(aph.ai_power_user_rate, 1) as ai_power_user_percentage,
    
    -- Content Performance
    ch.active_cards_count,
    round(ch.avg_content_like_rate, 1) as content_like_rate,
    round(ch.avg_content_conversion_rate, 1) as content_conversion_rate,
    ch.high_performing_cards,
    
    -- Multiplayer Feature
    mh.total_multiplayer_sessions_last_30,
    round(mh.avg_participants_per_session, 1) as avg_multiplayer_participants,
    round(mh.avg_consensus_rate, 1) as multiplayer_consensus_rate,
    
    -- Period-over-period growth
    round(((cpm.avg_daily_users / nullif(ppm.avg_daily_users, 0)) - 1) * 100, 1) as dau_growth_vs_previous_30d,
    round(((cpm.avg_ai_adoption / nullif(ppm.avg_ai_adoption, 0)) - 1) * 100, 1) as ai_adoption_growth_vs_previous_30d,
    
    -- Health Indicators
    bhs.overall_health_score,
    bhs.growth_health,
    bhs.ai_health,
    
    -- Engagement Quality
    round(cpm.avg_events_per_user, 1) as avg_events_per_user,
    round(cpm.avg_ai_adoption, 1) as ai_adoption_rate_percent,
    round(cpm.avg_conversion_rate, 1) as conversion_user_rate_percent

from current_period_metrics cpm
cross join previous_period_metrics ppm
cross join acquisition_health ah  
cross join ai_performance_health aph
cross join content_health ch
cross join multiplayer_health mh
cross join business_health_scores bhs