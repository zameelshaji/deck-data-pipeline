-- Supplier performance metrics: impressions, intent, and conversion tracking
with supplier_cards as (
    -- Get all cards associated with suppliers
    select
        s.supplier_id,
        s.supplier_name,
        s.card_id,
        s.partnership_type,
        s.is_active,
        s.onboarding_date,
        s.commission_rate,
        c.name as card_name,
        c.category,
        c.rating,
        c.formatted_address
    from {{ ref('suppliers') }} s
    left join {{ ref('stg_cards') }} c on s.card_id = c.card_id::text
),

card_events as (
    -- Get all events for supplier cards
    select
        sc.supplier_id,
        sc.supplier_name,
        sc.card_id,
        sc.card_name,
        sc.partnership_type,
        sc.is_active,
        sc.commission_rate,
        e.user_id,
        e.event_name,
        e.event_category,
        e.event_source,
        e.event_timestamp,
        date(e.event_timestamp) as event_date,
        date_trunc('week', e.event_timestamp)::date as event_week,
        date_trunc('month', e.event_timestamp)::date as event_month
    from supplier_cards sc
    left join {{ ref('stg_events') }} e on sc.card_id = e.card_id
),

supplier_metrics as (
    select
        supplier_id,
        supplier_name,
        card_id,
        card_name,
        partnership_type,
        is_active,
        commission_rate,

        -- Impressions (views)
        count(case
            when event_category = 'Content Discovery'
                and event_name in ('impression', 'Impression', 'detail_view_open', 'detail_open')
            then 1
        end) as total_impressions,

        count(distinct case
            when event_category = 'Content Discovery'
                and event_name in ('impression', 'Impression', 'detail_view_open', 'detail_open')
            then user_id
        end) as unique_viewers,

        -- Intent actions (engagement signals)
        count(case when event_name = 'detail_view_open' then 1 end) as card_opens,
        count(case when event_name in ('swipe_right', 'saved') then 1 end) as positive_swipes,
        count(case when event_name = 'swipe_left' then 1 end) as negative_swipes,
        count(case when event_name = 'share' then 1 end) as shares,
        count(case when event_name = 'saved' then 1 end) as saves,

        -- Total intent actions
        count(case
            when event_name in ('detail_view_open', 'swipe_right', 'swipe_left', 'share', 'saved')
            then 1
        end) as total_intent_actions,

        count(distinct case
            when event_name in ('detail_view_open', 'swipe_right', 'swipe_left', 'share', 'saved')
            then user_id
        end) as unique_intent_users,

        -- Conversion actions (bottom of funnel)
        count(case when event_name = 'book_with_deck' then 1 end) as book_clicks,
        count(case when event_name = 'opened_website' then 1 end) as website_visits,
        count(case when event_name = 'click_directions' then 1 end) as direction_clicks,
        count(case when event_name = 'click_phone' then 1 end) as phone_clicks,

        -- Total conversions
        count(case when event_category = 'Conversion Action' then 1 end) as total_conversions,

        count(distinct case
            when event_category = 'Conversion Action'
            then user_id
        end) as unique_converting_users,

        -- Unique users and time metrics
        count(distinct user_id) as total_unique_users,
        count(distinct event_date) as days_with_activity,
        min(event_timestamp) as first_interaction,
        max(event_timestamp) as latest_interaction

    from card_events
    where event_timestamp is not null
    group by
        supplier_id,
        supplier_name,
        card_id,
        card_name,
        partnership_type,
        is_active,
        commission_rate
),

supplier_performance_calculated as (
    select
        *,

        -- Funnel conversion rates
        case
            when total_impressions > 0
            then round(100.0 * total_intent_actions / total_impressions, 2)
            else 0
        end as impression_to_intent_rate,

        case
            when total_intent_actions > 0
            then round(100.0 * total_conversions / total_intent_actions, 2)
            else 0
        end as intent_to_conversion_rate,

        case
            when total_impressions > 0
            then round(100.0 * total_conversions / total_impressions, 2)
            else 0
        end as overall_conversion_rate,

        -- CTR (Click-Through Rate) - impressions to card opens
        case
            when total_impressions > 0
            then round(100.0 * card_opens / total_impressions, 2)
            else 0
        end as click_through_rate,

        -- Engagement quality
        case
            when (positive_swipes + negative_swipes) > 0
            then round(100.0 * positive_swipes / (positive_swipes + negative_swipes), 2)
            else 0
        end as positive_swipe_rate,

        case
            when total_impressions > 0
            then round(100.0 * shares / total_impressions, 2)
            else 0
        end as share_rate,

        case
            when total_impressions > 0
            then round(100.0 * saves / total_impressions, 2)
            else 0
        end as save_rate,

        -- Engagement score (weighted)
        case
            when total_impressions > 0
            then round(
                (
                    (card_opens * 1) +
                    (positive_swipes * 2) +
                    (saves * 4) +
                    (shares * 3) +
                    (website_visits * 5) +
                    (book_clicks * 10)
                )::decimal / total_impressions,
                2
            )
            else 0
        end as engagement_score,

        -- User reach metrics
        case
            when total_unique_users > 0
            then round(total_impressions::decimal / total_unique_users, 2)
            else 0
        end as avg_impressions_per_user,

        case
            when unique_viewers > 0
            then round(100.0 * unique_converting_users / unique_viewers, 2)
            else 0
        end as user_conversion_rate

    from supplier_metrics
),

-- Time-based performance (last 7, 30, 90 days)
recent_performance as (
    select
        ce.supplier_id,

        -- Last 7 days
        count(distinct case
            when ce.event_timestamp >= current_date - interval '7 days'
                and ce.event_category = 'Content Discovery'
            then ce.user_id
        end) as viewers_last_7d,

        count(case
            when ce.event_timestamp >= current_date - interval '7 days'
                and ce.event_category = 'Conversion Action'
            then 1
        end) as conversions_last_7d,

        -- Last 30 days
        count(distinct case
            when ce.event_timestamp >= current_date - interval '30 days'
                and ce.event_category = 'Content Discovery'
            then ce.user_id
        end) as viewers_last_30d,

        count(case
            when ce.event_timestamp >= current_date - interval '30 days'
                and ce.event_category = 'Conversion Action'
            then 1
        end) as conversions_last_30d,

        -- Last 90 days
        count(distinct case
            when ce.event_timestamp >= current_date - interval '90 days'
                and ce.event_category = 'Content Discovery'
            then ce.user_id
        end) as viewers_last_90d,

        count(case
            when ce.event_timestamp >= current_date - interval '90 days'
                and ce.event_category = 'Conversion Action'
            then 1
        end) as conversions_last_90d

    from card_events ce
    where ce.event_timestamp is not null
    group by ce.supplier_id
)

-- Final output
select
    spc.*,
    rp.viewers_last_7d,
    rp.conversions_last_7d,
    rp.viewers_last_30d,
    rp.conversions_last_30d,
    rp.viewers_last_90d,
    rp.conversions_last_90d,

    -- Performance tier classification
    case
        when spc.engagement_score >= 8 then 'Top Performer'
        when spc.engagement_score >= 5 then 'Strong Performer'
        when spc.engagement_score >= 3 then 'Average Performer'
        when spc.engagement_score >= 1 then 'Underperforming'
        else 'No Engagement'
    end as performance_tier,

    -- Days since launch
    extract(day from (current_date - sc.onboarding_date)) as days_since_onboarding

from supplier_performance_calculated spc
left join recent_performance rp on spc.supplier_id = rp.supplier_id
left join supplier_cards sc on spc.supplier_id = sc.supplier_id
order by spc.engagement_score desc, spc.total_impressions desc
