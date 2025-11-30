{{ config(materialized='table') }}

-- 1) Extract interest events (likes + saves)
with interest_events AS (
    SELECT
        user_id,
        card_id::text AS card_id,
        event_name,
        event_timestamp
    FROM {{ ref('stg_events_intent') }}
    WHERE event_name IN ('swipe_right', 'saved')
),

-- 2) Extract conversion events
conversion_events AS (
    SELECT
        user_id,
        card_id::text AS card_id,
        event_name AS conversion_event,
        event_timestamp AS conversion_timestamp
    FROM {{ ref('stg_events_intent') }}
    WHERE event_name IN (
        'opened_website',
        'click_directions',
        'click_phone',
        'book_button_click',
        'book_with_deck'
    )
),

-- 3) Match interest → conversion for same user + same card
joined AS (
    SELECT
        i.user_id,
        i.card_id,
        i.event_name AS interest_event,
        COUNT(c.card_id) > 0 AS converted
    FROM interest_events i
    LEFT JOIN conversion_events c
        ON i.user_id = c.user_id
       AND i.card_id = c.card_id
       AND c.conversion_timestamp > i.event_timestamp   -- conversion after interest
    GROUP BY 1,2,3
)

-- 4) Aggregate
SELECT
    -- LIKE funnel
    SUM(CASE WHEN interest_event = 'swipe_right' THEN 1 END) AS total_likes,
    SUM(CASE WHEN interest_event = 'swipe_right' AND converted THEN 1 END) AS like_to_conversion,

    -- SAVE funnel
    SUM(CASE WHEN interest_event = 'saved' THEN 1 END) AS total_saves,
    SUM(CASE WHEN interest_event = 'saved' AND converted THEN 1 END) AS save_to_conversion

FROM joined
