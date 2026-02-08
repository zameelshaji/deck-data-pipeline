{{ config(materialized='table') }}

-- Silver model: Staging table for onboarding events
-- Filters app_events to only onboarding-related events and extracts relevant properties

with onboarding_events as (
    select
        ae.id,
        ae.event_name,
        ae.event_timestamp,
        ae.user_id,
        -- Extract permission_granted from properties JSON
        (ae.properties->>'permission_granted')::boolean as permission_granted,
        -- Extract feature selected from properties JSON
        ae.properties->>'feature' as feature_selected,
        date(ae.event_timestamp) as event_date
    from {{ ref('src_app_events') }} ae
    inner join {{ ref('stg_users') }} u
        on ae.user_id = u.user_id
    where ae.event_name like 'onboarding_%'
      and u.is_test_user = 0
)

select
    id,
    event_name,
    event_timestamp,
    user_id,
    permission_granted,
    feature_selected,
    event_date
from onboarding_events
order by user_id, event_timestamp
