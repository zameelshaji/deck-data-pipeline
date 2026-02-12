-- Verify cohort key is always activation_week, never signup_week
-- All cohort-based models must segment by activation_week

-- Check fct_cohort_quality uses activation_week as grain
select *
from {{ ref('fct_cohort_quality') }}
where activation_week is null
