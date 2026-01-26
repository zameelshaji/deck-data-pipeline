-- Multiplayer session creation and participation events
-- Treated as separate social engagement type (NOT a share proxy)

with session_creates as (
    -- Users who created multiplayer sessions
    select
        creator_id as user_id,
        multiplayer_id,
        'multiplayer_create' as action_type,
        created_at as action_at,
        source_type,
        source_board_id,
        ai_prompt is not null as is_ai_generated,
        1 as is_creator
    from {{ ref('src_multiplayer_sessions') }}
    where creator_id is not null
),

session_joins as (
    -- Users who joined multiplayer sessions (excluding creators to avoid double-counting)
    select
        sp.user_id,
        sp.multiplayer_id,
        'multiplayer_join' as action_type,
        sp.joined_at as action_at,
        ms.source_type,
        ms.source_board_id,
        ms.ai_prompt is not null as is_ai_generated,
        0 as is_creator
    from {{ ref('src_session_participants') }} sp
    inner join {{ ref('src_multiplayer_sessions') }} ms
        on sp.multiplayer_id = ms.multiplayer_id
    where sp.is_creator = false
      and sp.user_id is not null
),

combined_actions as (
    select * from session_creates
    union all
    select * from session_joins
)

select
    -- Generate unique action_id
    md5(user_id::text || '-' || multiplayer_id::text || '-' || action_type || '-' || action_at::text) as action_id,
    user_id,
    multiplayer_id,
    action_type,
    action_at,
    source_type,
    source_board_id,
    is_ai_generated,
    is_creator,
    date(action_at) as action_date,
    date_trunc('week', action_at)::date as action_week
from combined_actions
