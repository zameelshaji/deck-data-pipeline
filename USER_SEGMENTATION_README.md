# User Segmentation: Planners vs Passengers

## Overview

This customer segmentation model identifies two critical user types:

- **Planners**: Users who actively initiate planning and take responsibility for deciding where to go
- **Passengers**: Users who engage passively when invited but don't initiate planning

## Why This Matters

**If planners don't come back, nothing else matters.** This segmentation is the primary PMF signal for DECK.

## Key Findings (Current Data)

### Overall Breakdown
- **Total Users**: 1,177 (excluding test accounts)
- **Planners**: 104 users (8.84%)
- **Passengers**: 1,073 users (91.16%)

### Engagement Comparison

| Metric | Planners | Passengers | Ratio |
|--------|----------|------------|-------|
| Activation Rate | 98.08% | 34.20% | 2.9x |
| Avg Prompts/Active Day | 2.00 | 0.23 | 8.9x |
| Avg Swipes/Prompt | 3.51 | 0.58 | 6.0x |
| Avg Conversion Rate | 2.85% | 0.06% | 44.1x |
| Avg Days Active | 3.7 | 0.6 | 6.2x |

### Planner Qualification Criteria

| Criteria | Users | % of Planners |
|----------|-------|---------------|
| Prompted 2+ times AND swiped 3+ cards | 84 | 80.77% |
| Created at least one deck | 38 | 36.54% |
| Created multiplayer session | 14 | 13.46% |
| Shared content | 10 | 9.62% |

**Note**: 30 Planners (28.85%) met multiple criteria, indicating high engagement.

## Data Models

### 1. `user_segmentation` (analytics_prod_gold.user_segmentation)

**Location**: [models/gold/user_segmentation.sql](models/gold/user_segmentation.sql)

User-level segmentation with all engagement metrics. Each row represents one user.

**Key Columns**:
- `user_type`: 'Planner' or 'Passenger'
- `planner_criteria_met`: Array of criteria that qualified them as a Planner
- Engagement metrics: prompts, swipes, saves, shares, decks created, multiplayer sessions
- Quality metrics: prompts_per_active_day, swipes_per_prompt, conversion_rate

**Sample Query**:
```sql
-- Get all Planners who created decks
SELECT username, email, planner_criteria_met, total_decks_created, days_active
FROM analytics_prod_gold.user_segmentation
WHERE user_type = 'Planner'
  AND 'created_deck' = ANY(planner_criteria_met)
ORDER BY days_active DESC;
```

### 2. `user_segmentation_summary` (analytics_prod_gold.user_segmentation_summary)

**Location**: [models/gold/user_segmentation_summary.sql](models/gold/user_segmentation_summary.sql)

Single-row summary view for dashboards and executive reporting.

**Sample Query**:
```sql
SELECT * FROM analytics_prod_gold.user_segmentation_summary;
```

## Verification & Analysis

### Run Verification Script

```bash
source venv/bin/activate
python3 verify_segmentation.py
```

This runs 10 validation queries covering:
1. Planner vs Passenger breakdown
2. Criteria distribution
3. Sample users from each segment
4. Edge cases (zero activity, recent signups, test users)
5. Engagement quality comparisons
6. Weekly cohort analysis

### Quick Summary

```bash
source venv/bin/activate
python3 show_summary.py
```

### dbt Commands

```bash
# Rebuild both models
dbt run --select user_segmentation user_segmentation_summary

# Run just segmentation
dbt run --select user_segmentation

# Run tests
dbt test --select user_segmentation
```

## Business Logic

### Planner Qualification

A user is classified as a **Planner** if they meet **ANY** of these conditions:

1. **Prompt & Swipe**: Prompted Dextr at least 2 times AND swiped through at least 3 recommendations
2. **Deck Creation**: Created at least one non-default board/deck
3. **Content Sharing**: Shared at least one deck or card
4. **Multiplayer**: Created at least one multiplayer planning session

Otherwise, they are a **Passenger**.

### Test User Exclusion

All users where `is_test_user = 1` are automatically excluded from the segmentation.

## Data Sources

The model aggregates data from:

- `stg_users`: User profiles with onboarding status
- `stg_events`: Unified event stream (prompts, swipes, saves, shares, conversions)
- `src_dextr_queries`: AI prompt history
- `src_boards`: User-created decks/wishlists
- `stg_multiplayer`: Multiplayer session details
- `src_core_card_actions`: Card-level actions including shares
- `src_session_participants`: Multiplayer participation

## Use Cases

### 1. Retention Analysis
```sql
-- Planner retention by signup cohort
SELECT
    DATE_TRUNC('week', created_at) as signup_week,
    COUNT(*) as planners,
    AVG(days_active) as avg_days_active,
    AVG(days_since_last_activity) as avg_recency
FROM analytics_prod_gold.user_segmentation
WHERE user_type = 'Planner'
GROUP BY 1
ORDER BY 1 DESC;
```

### 2. Conversion Funnel
```sql
-- Users close to becoming Planners
SELECT
    COUNT(*) as almost_planners
FROM analytics_prod_gold.user_segmentation
WHERE user_type = 'Passenger'
  AND (total_prompts >= 1 OR total_swipes >= 2);
```

### 3. Product Usage Insights
```sql
-- Which criteria drive the most engaged Planners?
SELECT
    planner_criteria_met,
    COUNT(*) as planners,
    AVG(conversion_rate) as avg_conversion_rate,
    AVG(days_active) as avg_days_active
FROM analytics_prod_gold.user_segmentation
WHERE user_type = 'Planner'
GROUP BY 1
ORDER BY avg_days_active DESC;
```

### 4. Dashboard Metrics
```sql
-- Key metrics for executive dashboard
SELECT
    total_planners,
    planner_percentage || '%' as planner_pct,
    planner_activation_rate || '%' as activation_rate,
    planner_vs_passenger_conversion_ratio || 'x' as conversion_advantage
FROM analytics_prod_gold.user_segmentation_summary;
```

## Future Enhancements (Answered Questions)

### 1. Should we add `days_to_become_planner`?

**Recommendation**: Yes, this would be valuable for understanding time-to-activation.

**Implementation**:
```sql
-- Add to user_segmentation model
MIN(event_timestamp) FILTER (WHERE meets_planner_criteria) - created_at as days_to_become_planner
```

### 2. Should we track recency (active vs churned Planners)?

**Recommendation**: Yes, critical for retention focus.

**Implementation**:
```sql
-- Add derived field
CASE
    WHEN user_type = 'Planner' AND days_since_last_activity <= 7 THEN 'Active Planner'
    WHEN user_type = 'Planner' AND days_since_last_activity > 30 THEN 'Churned Planner'
    WHEN user_type = 'Planner' THEN 'At-Risk Planner'
    ELSE user_type
END as detailed_segment
```

### 3. Should we track when users first became Planners?

**Recommendation**: Yes, useful for cohort analysis.

**Implementation**:
```sql
-- Add timestamp field
MIN(event_timestamp) as became_planner_at
```

## Files Created

1. **Models**:
   - [models/gold/user_segmentation.sql](models/gold/user_segmentation.sql) - Main segmentation model
   - [models/gold/user_segmentation_summary.sql](models/gold/user_segmentation_summary.sql) - Summary view

2. **Documentation**:
   - [models/gold/_schema.yml](models/gold/_schema.yml) - Updated with full documentation

3. **Analysis & Verification**:
   - [analyses/user_segmentation_verification.sql](analyses/user_segmentation_verification.sql) - 12 verification queries
   - [verify_segmentation.py](verify_segmentation.py) - Python script to run validations
   - [show_summary.py](show_summary.py) - Quick summary display

4. **Documentation**:
   - [USER_SEGMENTATION_README.md](USER_SEGMENTATION_README.md) - This file

## Next Steps

1. **Add to Dashboard**: Integrate `user_segmentation_summary` into your Streamlit dashboard
2. **Set Up Alerts**: Monitor Planner % by week for early warning of PMF issues
3. **Retention Analysis**: Build cohort retention curves specifically for Planners
4. **Activation Campaigns**: Target Passengers who are "almost Planners" (1 prompt, 2 swipes)
5. **Churn Prevention**: Identify at-risk Planners (no activity in 14+ days)

## Support

For questions or issues, contact the data team or refer to:
- dbt documentation in [models/gold/_schema.yml](models/gold/_schema.yml)
- Verification queries in [analyses/user_segmentation_verification.sql](analyses/user_segmentation_verification.sql)
