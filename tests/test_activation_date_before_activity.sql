-- Verify activation_date is always <= last_activity_date
-- An activated user's activation should never be after their last activity

select *
from {{ ref('fct_user_segments') }}
where is_activated = true
  and activation_date is not null
  and last_activity_date is not null
  and activation_date > last_activity_date
