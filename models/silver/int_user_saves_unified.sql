-- Unified save events with deduplication across core_card_actions and board_places_v2
-- Uses 5-minute window to deduplicate same user+card saves appearing in both sources

with core_card_saves as (
    -- Saves from core_card_actions (direct saves)
    select
        user_id,
        card_id::text as card_id,
        'direct_save' as save_source,
        source as surface_source,
        source_id::text as surface_id,
        timestamp as saved_at,
        'core_card_actions' as original_source
    from {{ ref('src_core_card_actions') }}
    where action_type = 'saved'
      and user_id is not null
),

board_saves as (
    -- Saves from board_places_v2 (board adds)
    -- Use added_by if available, otherwise fall back to board owner
    select
        coalesce(bp.added_by, b.user_id) as user_id,
        coalesce(bp.place_deck_sku, bp.place_id::text) as card_id,
        'board_add' as save_source,
        'board' as surface_source,
        bp.board_id::text as surface_id,
        bp.added_at as saved_at,
        'board_places_v2' as original_source
    from {{ ref('src_board_places_v2') }} bp
    inner join {{ ref('src_boards') }} b on bp.board_id = b.id
    where coalesce(bp.added_by, b.user_id) is not null
),

combined_saves as (
    select * from core_card_saves
    union all
    select * from board_saves
),

-- Add row numbers and lag timestamps for deduplication
saves_with_dedup_info as (
    select
        *,
        row_number() over (
            partition by user_id, card_id
            order by saved_at
        ) as save_sequence,
        lag(saved_at) over (
            partition by user_id, card_id
            order by saved_at
        ) as prev_save_at,
        lag(original_source) over (
            partition by user_id, card_id
            order by saved_at
        ) as prev_source
    from combined_saves
),

-- Apply 5-minute deduplication window
-- Keep first save, and any subsequent save that is either:
-- 1. More than 5 minutes after the previous save, OR
-- 2. From the same source (allow multiple saves from same source)
deduplicated as (
    select
        user_id,
        card_id,
        save_source,
        surface_source,
        surface_id,
        saved_at,
        original_source,
        save_sequence,
        -- Flag if this save had a matching record in another source within 5 min
        case
            when prev_save_at is not null
                and original_source != prev_source
                and extract(epoch from (saved_at - prev_save_at)) <= 300
            then true
            else false
        end as dedup_window_match,
        -- Flag if we're keeping this as the deduplicated record
        case
            when save_sequence = 1 then false  -- First save, not deduplicated
            when prev_save_at is null then false
            when original_source = prev_source then false  -- Same source, keep both
            when extract(epoch from (saved_at - prev_save_at)) > 300 then false  -- Gap > 5 min
            else true  -- This is a duplicate within 5 min from different source
        end as is_duplicate
    from saves_with_dedup_info
)

select
    -- Generate unique save_id
    md5(user_id::text || '-' || card_id || '-' || saved_at::text) as save_id,
    user_id,
    card_id,
    saved_at,
    save_source,
    surface_source as source_context,
    original_source,
    -- For the first occurrence, check if there was a duplicate within window
    coalesce(
        lead(dedup_window_match) over (
            partition by user_id, card_id
            order by saved_at
        ),
        false
    ) as is_deduplicated,
    dedup_window_match,
    date(saved_at) as save_date,
    date_trunc('week', saved_at)::date as save_week
from deduplicated
where is_duplicate = false
