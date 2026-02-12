-- AI (Dextr) feature performance and user satisfaction
with daily_ai_metrics as (
    select 
        date(query_timestamp) as query_date,
        
        -- Volume metrics
        count(*) as total_queries,
        count(distinct user_id) as unique_users,
        count(distinct pack_id) as unique_packs_generated,
        
        -- Performance metrics
        avg(processing_time_secs) as avg_processing_time,
        percentile_cont(0.95) within group (order by processing_time_secs) as p95_processing_time,
        
        -- Pack quality metrics
        avg(total_cards_in_pack) as avg_cards_per_pack,
        avg(cards_shown) as avg_cards_shown,
        avg(pack_completion_rate_percent) as avg_completion_rate,
        avg(engagement_rate_percent) as avg_engagement_rate,
        avg(like_rate_percent) as avg_like_rate,
        
        -- Success indicators
        count(case when pack_completion_rate_percent >= 50 then 1 end) as successful_packs,
        count(case when like_rate_percent >= 30 then 1 end) as well_liked_packs,
        count(case when cards_acted_upon >= 3 then 1 end) as engaged_sessions,
        
        -- User satisfaction proxies
        count(case when cards_liked >= 2 then 1 end) as sessions_with_multiple_likes,
        
        -- App version and location insights
        count(distinct location) as unique_locations,
        count(distinct app_version) as app_versions_used
        
    from {{ ref('stg_dextr_interactions') }}
    where pack_id is not null  -- Only successful queries that generated packs
    group by date(query_timestamp)
),

user_ai_behavior as (
    select 
        query_date,
        
        -- User engagement patterns
        count(case when total_queries_per_user = 1 then 1 end) as one_time_users,
        count(case when total_queries_per_user between 2 and 5 then 1 end) as casual_users,
        count(case when total_queries_per_user >= 6 then 1 end) as power_users,
        
        avg(avg_like_rate_per_user) as avg_user_satisfaction,
        
        -- Retention indicators
        count(case when days_between_first_last > 1 then 1 end) as returning_ai_users
        
    from (
        select 
            user_id,
            date(min(query_timestamp)) as query_date,
            count(*) as total_queries_per_user,
            avg(like_rate_percent) as avg_like_rate_per_user,
            max(date(query_timestamp)) - min(date(query_timestamp)) as days_between_first_last
        from {{ ref('stg_dextr_interactions') }}
        where pack_id is not null
        group by user_id
    ) user_stats
    group by query_date
),

rolling_trends as (
    select 
        dam.*,
        uab.one_time_users,
        uab.casual_users, 
        uab.power_users,
        uab.avg_user_satisfaction,
        uab.returning_ai_users,
        
        -- 7-day rolling averages for smoother trends
        avg(total_queries) over (
            order by dam.query_date 
            rows between 6 preceding and current row
        ) as queries_7day_avg,
        
        avg(avg_like_rate) over (
            order by dam.query_date 
            rows between 6 preceding and current row
        ) as satisfaction_7day_avg,
        
        avg(avg_processing_time) over (
            order by dam.query_date 
            rows between 6 preceding and current row
        ) as performance_7day_avg
        
    from daily_ai_metrics dam
    left join user_ai_behavior uab on dam.query_date = uab.query_date
)

select 
    *,
    
    -- Success rate calculations
    case when total_queries > 0 then round(100.0 * successful_packs / total_queries, 2) else 0 end as pack_success_rate,
    case when total_queries > 0 then round(100.0 * well_liked_packs / total_queries, 2) else 0 end as satisfaction_rate,
    case when total_queries > 0 then round(100.0 * engaged_sessions / total_queries, 2) else 0 end as engagement_success_rate,
    
    -- User behavior insights
    case when (one_time_users + casual_users + power_users) > 0 
         then round(100.0 * power_users / (one_time_users + casual_users + power_users), 2) 
         else 0 end as power_user_percentage,
    
    -- Queries per user
    case when unique_users > 0 then round(total_queries::decimal / unique_users, 2) else 0 end as queries_per_user,
    
    -- Performance benchmarks
    case when avg_processing_time <= 3 then 'Fast'
         when avg_processing_time <= 7 then 'Acceptable' 
         else 'Slow' end as performance_category

from rolling_trends
order by query_date desc