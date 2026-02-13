{{ config(materialized='table') }}

-- Place Identity Resolver
-- Resolves any card_id format to places.place_id (integer)
-- Priority: integer match → deck_sku match → google_place_id match → UUID match
--
-- card_id formats in the data:
--   1. Integer (e.g., "103")       → places.place_id or experience_cards.card_id
--   2. Deck SKU (e.g., "DECKC38367CT") → places.deck_sku
--   3. Google Place ID (e.g., "ChIJaer-OwAF...") → places.google_place_id
--   4. UUID (e.g., "060A0FC9-3540...") → board_places_v2.id (UUID PK) → place_id

with distinct_card_ids as (
    select distinct card_id
    from {{ ref('stg_unified_events') }}
    where card_id is not null
),

-- Places lookup table (all 3 identifiers populated)
places as (
    select
        place_id,
        google_place_id,
        deck_sku
    from {{ ref('src_places') }}
)

select
    ids.card_id as original_card_id,

    coalesce(
        -- 1. Integer card_id matching places.place_id directly
        case
            when ids.card_id ~ '^\d+$'
                 and p_int.place_id is not null
            then p_int.place_id
            else null
        end,
        -- 2. Deck SKU match (e.g., "DECKC38367CT")
        p_sku.place_id,
        -- 3. Google Place ID match (e.g., "ChIJ...")
        p_google.place_id,
        -- 4. UUID match via board_places_v2 → place_id
        case
            when ids.card_id ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-'
                 and bp.place_id is not null
            then bp.place_id
            else null
        end
    ) as resolved_place_id,

    case
        when ids.card_id ~ '^\d+$' and p_int.place_id is not null
            then 'integer'
        when p_sku.place_id is not null
            then 'deck_sku'
        when p_google.place_id is not null
            then 'google_place_id'
        when ids.card_id ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-'
             and bp.place_id is not null
            then 'uuid_board_place'
        else 'unresolved'
    end as resolution_method

from distinct_card_ids ids
-- Integer match: card_id is numeric and matches places.place_id
left join places p_int
    on ids.card_id ~ '^\d+$'
    and ids.card_id::integer = p_int.place_id
-- Deck SKU match
left join places p_sku
    on ids.card_id = p_sku.deck_sku
-- Google Place ID match
left join places p_google
    on ids.card_id = p_google.google_place_id
-- UUID match via board_places_v2
left join {{ ref('src_board_places_v2') }} bp
    on ids.card_id ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-'
    and ids.card_id = bp.id::text
