with events as (
    select
        date(event_timestamp) as day,
        event_name,
        intent_stage
    from {{ ref('stg_events_intent') }}  
)

select
    day,

    -- Totals per intent category
    sum(case when intent_stage = 'Interest'   then 1 else 0 end) as interest_total,
    sum(case when intent_stage = 'Conversion' then 1 else 0 end) as conversion_total,
    sum(case when intent_stage = 'Other'      then 1 else 0 end) as other_total,

    -- Event counts as columns
    sum(case when event_name = 'swipe_left'        then 1 else 0 end) as swipe_left_count,
    sum(case when event_name = 'swipe_right'       then 1 else 0 end) as swipe_right_count,
    sum(case when event_name = 'detail_view_open'  then 1 else 0 end) as detail_view_open_count,
    sum(case when event_name = 'detail_open'       then 1 else 0 end) as detail_open_count,
    sum(case when event_name = 'saved'             then 1 else 0 end) as saved_count,
    sum(case when event_name = 'share'             then 1 else 0 end) as share_count,
    sum(case when event_name in ('impression','Impression')
                                                then 1 else 0 end) as impression_count,
    sum(case when event_name = 'category_clicked' then 1 else 0 end) as category_clicked_count,
    sum(case when event_name = 'spotlight_click'  then 1 else 0 end) as spotlight_click_count,
    sum(case when event_name = 'opened_website'   then 1 else 0 end) as opened_website_count,
    sum(case when event_name = 'click_directions' then 1 else 0 end) as click_directions_count,
    sum(case when event_name = 'click_phone'      then 1 else 0 end) as click_phone_count,
    sum(case when event_name = 'book_button_click'then 1 else 0 end) as book_button_click_count,
    sum(case when event_name = 'book_with_deck'   then 1 else 0 end) as book_with_deck_count


from events
group by day
order by day desc 
