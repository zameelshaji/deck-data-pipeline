{{ config(materialized='table') }}

-- AI Prompt Performance Analysis
-- Grain: one row per prompt (query_id)
-- Answers: "Most common prompt types?", "Which prompts lead to 0 saves?",
--          "Are users re-prompting in the same session?",
--          "Do users who refine prompts retain better?"

with queries as (
    select
        di.query_id,
        di.user_id,
        di.query_text,
        di.query_timestamp,
        date(di.query_timestamp) as query_date,
        di.location,
        di.app_version,
        di.processing_time_secs,
        di.pack_id,
        di.pack_name,
        di.total_cards,
        di.cards_shown,
        di.cards_liked,
        di.cards_disliked,
        di.cards_acted_upon
    from {{ ref('stg_dextr_interactions') }} di
    -- Exclude test users
    inner join {{ ref('stg_users') }} u on di.user_id = u.user_id
    where u.is_test_user = 0
),

-- Map queries to sessions via timestamp overlap
query_sessions as (
    select
        q.query_id,
        s.session_id,
        s.session_date,
        s.has_save,
        s.has_share,
        s.has_post_share_interaction,
        s.save_count as session_save_count,
        s.share_count as session_share_count
    from queries q
    inner join {{ ref('fct_session_outcomes') }} s
        on q.user_id = s.user_id
        and q.query_timestamp between s.started_at and s.ended_at
),

-- Count saves attributed to each pack (from board_places_v2 + places matching)
pack_saves as (
    select
        q.query_id,
        count(distinct bp.place_id) as cards_saved
    from queries q
    inner join {{ ref('src_dextr_packs') }} dp on q.pack_id = dp.pack_id
    left join {{ ref('src_dextr_pack_cards') }} dpc on dp.pack_id = dpc.pack_id
    left join {{ ref('src_board_places_v2') }} bp
        on bp.added_by = q.user_id
        and bp.place_id::text = dpc.card_id::text
        and bp.added_at >= q.query_timestamp
        and bp.added_at < q.query_timestamp + interval '1 hour'
    group by q.query_id
),

-- Prompt sequencing within session
prompt_sequencing as (
    select
        qs.query_id,
        qs.session_id,
        row_number() over (
            partition by qs.session_id
            order by q.query_timestamp
        ) as prompt_sequence_in_session,
        count(*) over (
            partition by qs.session_id
        ) as total_prompts_in_session
    from query_sessions qs
    inner join queries q on qs.query_id = q.query_id
),

-- User context from activation
user_context as (
    select
        user_id,
        activation_week,
        activation_type
    from {{ ref('fct_user_activation') }}
    where is_activated = true
),

-- User prior prompt count
user_prior_prompts as (
    select
        q1.query_id,
        count(q2.query_id) as user_total_prior_prompts
    from queries q1
    left join queries q2
        on q1.user_id = q2.user_id
        and q2.query_timestamp < q1.query_timestamp
    group by q1.query_id
),

-- User retention flag
user_retention as (
    select
        user_id,
        had_activity_d30 as user_retained_d30
    from {{ ref('fct_retention_activated') }}
)

select
    q.query_id,
    q.user_id,
    q.query_text,
    q.query_date,
    q.query_timestamp,
    q.location,
    q.app_version,

    -- Session context
    qs.session_id,
    coalesce(ps.prompt_sequence_in_session, 1) as prompt_sequence_in_session,
    ps.prompt_sequence_in_session > 1 as is_refinement,
    coalesce(ps.total_prompts_in_session, 1) as total_prompts_in_session,

    -- Results quality
    coalesce(q.total_cards, 0) as cards_generated,
    coalesce(q.cards_shown, 0) as cards_shown,
    coalesce(q.cards_liked, 0) as cards_liked,
    coalesce(q.cards_disliked, 0) as cards_disliked,
    coalesce(psv.cards_saved, 0) as cards_saved,
    case
        when coalesce(q.cards_acted_upon, 0) > 0
        then round(q.cards_liked::numeric / q.cards_acted_upon, 4)
        else null
    end as like_rate,
    case
        when coalesce(q.cards_shown, 0) > 0
        then round(coalesce(psv.cards_saved, 0)::numeric / q.cards_shown, 4)
        else null
    end as save_rate,
    coalesce(psv.cards_saved, 0) = 0 as zero_save_prompt,

    -- Processing time
    q.processing_time_secs,
    case
        when q.processing_time_secs is null then null
        when q.processing_time_secs < 3 then 'fast'
        when q.processing_time_secs <= 8 then 'normal'
        else 'slow'
    end as performance_category,

    -- Downstream outcomes (session-level)
    coalesce(qs.has_save, false) as led_to_save,
    coalesce(qs.has_share, false) as led_to_share,
    coalesce(qs.has_post_share_interaction, false) as led_to_post_share_interaction,

    -- User context
    uc.activation_week as user_activation_week,
    coalesce(upp.user_total_prior_prompts, 0) as user_total_prior_prompts,
    coalesce(ur.user_retained_d30, false) as user_retained_d30

from queries q
left join query_sessions qs on q.query_id = qs.query_id
left join prompt_sequencing ps on q.query_id = ps.query_id
left join pack_saves psv on q.query_id = psv.query_id
left join user_context uc on q.user_id = uc.user_id
left join user_prior_prompts upp on q.query_id = upp.query_id
left join user_retention ur on q.user_id = ur.user_id
