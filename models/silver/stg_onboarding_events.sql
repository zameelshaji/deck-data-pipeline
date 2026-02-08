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
),

version_lookup as (
    select
        app_version::text as app_version,
        release_date::date as release_date,
        release_date_end::date as release_date_end
    from {{ ref('app_version_releases') }}
)

select
    oe.id,
    oe.event_name,
    oe.event_timestamp,
    oe.user_id,
    oe.permission_granted,
    oe.feature_selected,
    oe.event_date,
    v.app_version as imputed_app_version
from onboarding_events oe
left join version_lookup v
    on oe.event_date between v.release_date and v.release_date_end
order by oe.user_id, oe.event_timestamp
