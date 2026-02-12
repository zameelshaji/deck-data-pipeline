-- Content performance across all card types and sources
with card_engagement as (
    select
        c.card_id,
        c.card_type,
        c.name as card_name,
        c.category,
        c.source_type,
        c.rating,
        c.price_level,
        c.formatted_address,

        -- Engagement metrics
        count(case when e.event_category = 'Swipe' then 1 end) as impressions,
        count(case when e.event_type in ('swipe_right', 'save') then 1 end) as positive_actions,
        count(case when e.event_type = 'swipe_left' then 1 end) as negative_actions,
        count(case when e.event_category = 'Conversion' then 1 end) as conversions,
        count(case when e.event_category = 'Share' then 1 end) as shares,
        count(case when e.event_type = 'save' then 1 end) as saves,
        count(case when e.event_type = 'conversion' and e.source_table = 'core_card_actions' then 1 end) as bookings,
        count(case when e.event_type = 'conversion' then 1 end) as website_visits,

        count(distinct e.user_id) as unique_users_engaged,
        count(distinct date(e.event_timestamp)) as days_with_activity,

        min(e.event_timestamp) as first_interaction,
        max(e.event_timestamp) as latest_interaction

    from {{ ref('stg_cards') }} c
    left join {{ ref('stg_unified_events') }} e on c.card_id::text = e.card_id
    group by c.card_id, c.card_type, c.name, c.category, c.source_type, c.rating, c.price_level, c.formatted_address
),

daily_content_metrics as (
    select
        date(e.event_timestamp) as activity_date,
        e.source_table as content_source,
        c.card_type,

        count(distinct e.card_id) as unique_cards_interacted,
        count(case when e.event_category = 'Swipe' then 1 end) as daily_impressions,
        count(case when e.event_type in ('swipe_right', 'save') then 1 end) as daily_positive_actions,
        count(case when e.event_category = 'Conversion' then 1 end) as daily_conversions,
        count(distinct e.user_id) as daily_content_users,

        avg(case when e.event_type = 'swipe_right' then 1.0
                 when e.event_type = 'swipe_left' then 0.0 end) as daily_like_rate

    from {{ ref('stg_unified_events') }} e
    left join {{ ref('stg_cards') }} c on c.card_id::text = e.card_id
    where e.card_id is not null
    group by date(e.event_timestamp), e.source_table, c.card_type
),

content_performance_summary as (
    select
        ce.*,

        case when impressions > 0 then round(100.0 * positive_actions / impressions, 2) else 0 end as positive_rate,
        case when (positive_actions + negative_actions) > 0
             then round(100.0 * positive_actions / (positive_actions + negative_actions), 2)
             else 0 end as like_rate,
        case when impressions > 0 then round(100.0 * conversions / impressions, 2) else 0 end as conversion_rate,
        case when positive_actions > 0 then round(100.0 * conversions / positive_actions, 2) else 0 end as positive_to_conversion_rate,
        case when impressions > 0 then round(100.0 * shares / impressions, 2) else 0 end as share_rate,
        case when impressions > 0 then round(100.0 * saves / impressions, 2) else 0 end as save_rate,

        case when impressions > 0 then
            round((
                (positive_actions * 2) +
                (conversions * 5) +
                (shares * 3) +
                (saves * 4) +
                (bookings * 10)
            )::decimal / impressions, 2)
        else 0 end as engagement_score,

        case when positive_actions > 0 then round(shares::decimal / positive_actions, 3) else 0 end as virality_coefficient,
        case when unique_users_engaged > 0 then round(days_with_activity::decimal / unique_users_engaged, 2) else 0 end as stickiness_ratio

    from card_engagement ce
),

top_performers as (
    select
        card_type,
        source_type,
        category,

        count(*) as total_cards,
        avg(impressions) as avg_impressions,
        avg(engagement_score) as avg_engagement_score,
        avg(like_rate) as avg_like_rate,
        avg(conversion_rate) as avg_conversion_rate,

        percentile_cont(0.9) within group (order by engagement_score) as top_10_percent_engagement,
        percentile_cont(0.5) within group (order by like_rate) as median_like_rate,

        count(case when engagement_score >= 5 then 1 end) as high_engagement_cards,
        count(case when like_rate >= 70 then 1 end) as well_liked_cards,
        count(case when conversion_rate >= 5 then 1 end) as high_converting_cards

    from content_performance_summary
    where impressions >= 10
    group by card_type, source_type, category
)

select
    'individual_cards' as metric_type,
    card_id::text as dimension_value,
    card_name as dimension_label,
    card_type,
    source_type,
    category,
    impressions,
    positive_actions,
    conversions,
    like_rate,
    conversion_rate,
    engagement_score,
    null::decimal as avg_engagement_score,
    null::integer as total_cards
from content_performance_summary
where impressions >= 5

union all

select
    'category_performance' as metric_type,
    coalesce(category, 'uncategorized') as dimension_value,
    coalesce(category, 'Uncategorized') as dimension_label,
    card_type,
    source_type,
    null as category,
    avg_impressions::integer as impressions,
    null::bigint as positive_actions,
    null::bigint as conversions,
    avg_like_rate as like_rate,
    avg_conversion_rate as conversion_rate,
    null::decimal as engagement_score,
    avg_engagement_score,
    total_cards
from top_performers

order by metric_type, engagement_score desc nulls last, avg_engagement_score desc nulls last
