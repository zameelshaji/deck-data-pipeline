-- Card-level share events from core_card_actions
-- NOTE: This captures card shares only, NOT multiplayer (that's separate)
-- deck_shared events are NOT instrumented - this is a known data gap

with card_shares as (
    select
        user_id,
        card_id::text as card_id,
        source as share_surface,
        source_id::text as surface_id,
        timestamp as shared_at
    from {{ ref('src_core_card_actions') }}
    where action_type = 'share'
      and user_id is not null
)

select
    -- Generate unique share_id
    md5(user_id::text || '-' || card_id || '-' || shared_at::text) as share_id,
    user_id,
    card_id,
    shared_at,
    share_surface,
    surface_id as source_context,
    date(shared_at) as share_date,
    date_trunc('week', shared_at)::date as share_week
from card_shares
