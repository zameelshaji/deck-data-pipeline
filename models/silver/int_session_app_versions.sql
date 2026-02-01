{{ config(materialized='table') }}

with sessions as (
    select
        session_id,
        session_date,
        app_version as native_app_version
    from {{ ref('fct_session_outcomes') }}
),

version_lookup as (
    select
        app_version,
        release_date::date as release_date,
        release_date_end::date as release_date_end
    from {{ ref('app_version_releases') }}
)

select
    s.session_id,
    s.session_date,
    s.native_app_version,
    v.app_version as imputed_app_version,
    coalesce(s.native_app_version, v.app_version) as effective_app_version,
    s.native_app_version is not null as has_native_version
from sessions s
left join version_lookup v
    on s.session_date between v.release_date and v.release_date_end
