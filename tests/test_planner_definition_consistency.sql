-- Verify Planner definition: saved AND shared at least once
-- A planner must have total_saves > 0 AND total_shares > 0
-- A passenger must be activated but NOT a planner

select *
from {{ ref('fct_user_segments') }}
where
    -- Planners who don't have both saves and shares (violation)
    (is_planner = true and (total_saves = 0 or total_shares = 0))
    -- Passengers who DO have both saves and shares (violation)
    or (is_passenger = true and total_saves > 0 and total_shares > 0)
    -- Activated users who are neither planner nor passenger (violation)
    or (is_activated = true and is_planner = false and is_passenger = false)
