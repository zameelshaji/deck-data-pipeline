-- Historical headline metrics - cumulative growth over time
with date_spine as (
    -- Generate a row for each date from first user signup to today
    select 
        generate_series(
            (select min(created_at)::date from {{ ref('stg_users') }}),
            current_date,
            '1 day'::interval
        )::date as metric_date
),

cumulative_users as (
    select 
        ds.metric_date,
        count(distinct u.user_id) as total_users
    from date_spine ds
    left join {{ ref('stg_users') }} u 
        on u.created_at::date <= ds.metric_date
    group by ds.metric_date
),

cumulative_cards as (
    select 
        ds.metric_date,
        count(distinct c.card_id) as total_experience_cards,
        count(distinct case when c.card_type = 'experience' then c.card_id end) as ai_generated_cards,
        count(distinct case when c.card_type = 'featured' then c.card_id end) as featured_partnership_cards
    from date_spine ds
    left join {{ ref('stg_cards') }} c 
        on c.created_at::date <= ds.metric_date
    group by ds.metric_date
),

cumulative_prompts as (
    select 
        ds.metric_date,
        count(dq.query_id) + count(lq.query) as total_prompts,
        count(distinct dq.user_id) as users_who_prompted
    from date_spine ds
    left join {{ ref('src_dextr_queries') }} dq 
        on dq.query_timestamp::date <= ds.metric_date
    left join {{ ref('src_learned_places_queries')}} lq 
        on lq.created_at::date <= ds.metric_date
    group by ds.metric_date
),

cumulative_swipes as (
    select 
        ds.metric_date,
        count(case when e.event_name in ('swipe_right', 'swipe_left') then 1 end) as total_swipes,
        count(case when e.event_name = 'swipe_right' then 1 end) as total_swipes_right,
        count(case when e.event_name = 'swipe_left' then 1 end) as total_swipes_left,
        count(distinct case when e.event_name in ('swipe_right', 'swipe_left') then e.user_id end) as users_who_swiped
    from date_spine ds
    left join {{ ref('stg_events') }} e 
        on e.event_timestamp::date <= ds.metric_date
    group by ds.metric_date
),

daily_new_metrics as (
    select 
        ds.metric_date,
        
        -- New users on this day
        count(distinct case when u.created_at::date = ds.metric_date then u.user_id end) as new_users_today,
        
        -- New cards on this day
        count(distinct case when c.created_at::date = ds.metric_date then c.card_id end) as new_cards_today,
        
        -- Prompts on this day
        count(case when dq.query_timestamp::date = ds.metric_date then 1 end) as prompts_today,
        
        -- Swipes on this day
        count(case 
            when e.event_timestamp::date = ds.metric_date 
            and e.event_name in ('swipe_right', 'swipe_left') 
            then 1 
        end) as swipes_today,
        
        -- Active users on this day
        count(distinct case when e.event_timestamp::date = ds.metric_date then e.user_id end) as active_users_today
        
    from date_spine ds
    left join {{ ref('stg_users') }} u on u.created_at::date = ds.metric_date
    left join {{ ref('stg_cards') }} c on c.created_at::date = ds.metric_date
    left join {{ ref('src_dextr_queries') }} dq on dq.query_timestamp::date = ds.metric_date
    left join {{ ref('stg_events') }} e on e.event_timestamp::date = ds.metric_date
    group by ds.metric_date
)

select 
    ds.metric_date,
    
    -- Cumulative totals (the big 4)
    cu.total_users,
    cc.total_experience_cards,
    cp.total_prompts,
    cs.total_swipes,
    
    -- Cumulative breakdowns
    cc.ai_generated_cards,
    cc.featured_partnership_cards,
    cs.total_swipes_right,
    cs.total_swipes_left
   
from date_spine ds
left join cumulative_users cu on ds.metric_date = cu.metric_date
left join cumulative_cards cc on ds.metric_date = cc.metric_date
left join cumulative_prompts cp on ds.metric_date = cp.metric_date
left join cumulative_swipes cs on ds.metric_date = cs.metric_date
left join daily_new_metrics dm on ds.metric_date = dm.metric_date

-- Only include dates where there's actual data
where cu.total_users > 0 or cc.total_experience_cards > 0

order by ds.metric_date