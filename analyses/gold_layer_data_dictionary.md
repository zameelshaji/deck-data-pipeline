# DECK Gold Layer Data Dictionary

> For use in the Claude + Supabase MCP agent system prompt. All tables live in schema `analytics_prod_gold`. All tables exclude test users — no additional filtering needed.

## Key Business Definitions

| Term | Definition |
|------|-----------|
| **Activation** | First prompt, save, or share (deliberately low bar) |
| **Planner** | Activated user who has saved AND shared at least once (lifetime) |
| **Passenger** | Any activated user who is NOT a planner |
| **PSR (Broad)** | Plan Survival Rate = % sessions with save AND share |
| **Cohort Key** | Always `activation_week` (Monday of activation), never signup_week |
| **Retention (D7/D30/D60)** | Measured from `activation_date`, not signup. Activity = prompt, save, or share |
| **Churned** | No activity in 30+ days |

## User-Level Tables

### fct_user_segments
- **Purpose**: User archetypes and segmentation — the go-to table for "who are our users?"
- **Grain**: One row per non-test user
- **Key columns**: `user_id`, `user_archetype` (one_and_done/browser/saver/planner/power_planner), `is_planner`, `is_passenger`, `churn_risk` (high/medium/low), `is_churned`, `total_saves`, `total_shares`, `total_prompts`, `total_sessions`, `activation_date`, `activation_week`, `retained_d7/d30/d60`, `referral_source`, `prompt_to_save_rate`, `swipe_to_save_rate`

### fct_user_retention
- **Purpose**: Comprehensive retention fact table for CEO/executive reporting
- **Grain**: One row per activated user
- **Key columns**: `user_id`, `activation_date`, `cohort_week`, `user_type` (Planner/Passenger), `retained_d7/d30/d60/d90`, `sessions_d7/d30/d60/d90`, `total_prompts`, `total_saves`, `total_shares`, `is_churned`

## Engagement Tables

### fct_user_engagement_trajectory
- **Purpose**: Weekly engagement trends per user — shows how behavior evolves over time
- **Grain**: One row per user per activity week
- **Key columns**: `user_id`, `activity_week`, `activation_week`, `weeks_since_activation`, `sessions_count`, `prompts_count`, `saves_count`, `shares_count`, `cards_viewed`, `avg_cards_per_session`, `pct_sessions_with_save`, `pct_sessions_zero_actions`, `swipe_to_save_rate`, `cumulative_saves`, `cumulative_shares`

### fct_session_outcomes
- **Purpose**: Session-level North Star metric analysis with PSR ladder
- **Grain**: One row per planning session
- **Key columns**: `session_id`, `user_id`, `has_save`, `has_share`, `meets_psr_broad`, `meets_psr_strict`, `is_no_value_session`, `intent_strength`, `time_to_first_save_seconds`

## Cohort Table

### fct_cohort_quality
- **Purpose**: Investor-ready cohort analysis for board/exec reporting
- **Grain**: One row per activation week cohort
- **Key columns**: `activation_week`, `cohort_size`, `retention_rate_d7/d30/d60`, `planner_pct`, `one_and_done_pct`, `avg_first_session_saves`, `avg_first_session_swipes`, `avg_sessions_first_week`, `organic_retention_d30`, `referral_retention_d30`

## AI & Prompt Tables

### fct_prompt_analysis
- **Purpose**: Prompt-level performance — which prompts lead to saves/shares?
- **Grain**: One row per Dextr query
- **Key columns**: `query_id`, `user_id`, `query_text`, `prompt_sequence_in_session`, `is_refinement`, `cards_generated`, `cards_saved`, `save_rate`, `like_rate`, `zero_save_prompt`, `led_to_save`, `led_to_share`, `prompt_intent` (date_night/group_outing/dining/etc.), `prompt_specificity` (high/medium/low), `performance_category` (fast/normal/slow)

### fct_pack_performance
- **Purpose**: AI pack-level analytics — how well do generated packs perform?
- **Grain**: One row per pack
- **Key columns**: `pack_id`, `query_text`, `total_cards_generated`, `cards_saved`, `save_rate`, `like_rate`, `has_shortlist` (3+ saves), `share_rate`, `prompt_intent_category`

## Content & Place Tables

### fct_place_performance
- **Purpose**: Place/card-level performance — best/worst content in the catalog
- **Grain**: One row per place/card
- **Key columns**: `card_id`, `place_id`, `place_name`, `category`, `neighborhood`, `total_impressions`, `total_saves`, `total_shares`, `right_swipe_rate`, `save_rate`, `conversion_rate`, `viral_score`, `total_detail_views`, `total_book_clicks`, `impressions_last_7d/30d`, `saves_last_7d/30d`

## Conversion & Viral Tables

### fct_conversion_signals
- **Purpose**: Booking intent and monetisation readiness signals
- **Grain**: One row per conversion action (opened_website, book_with_deck, click_directions, click_phone)
- **Key columns**: `action_id`, `user_id`, `card_id`, `place_name`, `place_category`, `action_type`, `was_saved_first`, `time_from_save_to_conversion_minutes`, `was_prompt_initiated`, `prompt_text`

### fct_viral_loop
- **Purpose**: Social sharing and viral loop analytics — K-factor and share effectiveness
- **Grain**: One row per share link
- **Key columns**: `share_link_id`, `sharer_user_id`, `sharer_archetype`, `share_type` (board/card/multiplayer), `unique_viewers`, `viewers_who_signed_up`, `signup_conversion_rate`, `effective_k_factor`, `total_views`, `viewers_who_saved`, `time_to_first_view_minutes`, `time_to_first_signup_minutes`

## Supporting Tables (not for direct agent queries)

| Table | Purpose |
|-------|---------|
| `fct_north_star_daily` | Daily PSR/SSR/NVR aggregates with rolling averages |
| `fct_north_star_weekly` | Weekly PSR and WAP counts |
| `fct_activation_funnel` | User-level 7/30 day activation tracking |
| `fct_retention_activated` | User-level D7/D30/D60 retention flags |
| `fct_active_planners` | Weekly/monthly active planner counts |
| `fct_session_explorer` | Per-session JSONB diagnostic detail |
| `fct_user_activation` | User-level activation details |
| `fct_retention_by_cohort_week` | Weekly cohort retention rates |
| `fct_signup_to_activation_funnel` | Weekly signup → activation funnel |
| `vis_*` tables | Dashboard-specific views (6 tables), not for agent use |
