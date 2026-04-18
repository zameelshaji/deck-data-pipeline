-- Cohort retention floor estimation
--
-- Fits retention(t) = floor + (1 - floor) * exp(-t / tau) per cohort_week
-- to the four D7/D30/D60/D90 data points already computed in
-- fct_retention_by_cohort_week. The fitted "floor" is EQT's "steady-state
-- retention share" — the holy-grail number their framework asks for.
--
-- Method: a SQL grid search over discrete (tau, floor) pairs, selecting the
-- pair that minimises sum-of-squared-residuals at the four known points.
-- We stay in pure SQL (no dbt-py, no plpython) because the fit is
-- directional, not precision.
--
-- CAVEAT: Four data points per cohort is too few to fit precisely. Treat
-- fitted_floor as directional ±5pp, not exact. Reliability will improve as
-- D180 and D365 data become available; the grid and loss can be extended
-- trivially once those columns land in fct_retention_by_cohort_week.

with cohort_points as (
    select
        cohort_week,
        cohort_size,
        retention_rate_d7,
        retention_rate_d30,
        retention_rate_d60,
        retention_rate_d90
    from {{ ref('fct_retention_by_cohort_week') }}
    where mature_d90 >= 10
      and retention_rate_d7  is not null
      and retention_rate_d30 is not null
      and retention_rate_d60 is not null
      and retention_rate_d90 is not null
),

tau_grid as (
    select unnest(array[10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 365])::numeric as tau
),

floor_grid as (
    -- 0.00, 0.05, ..., 0.60
    select (generate_series(0, 60, 5))::numeric / 100.0 as floor_val
),

grid as (
    select tau, floor_val
    from tau_grid
    cross join floor_grid
),

candidates as (
    select
        c.cohort_week,
        c.cohort_size,
        g.tau,
        g.floor_val,
        power(c.retention_rate_d7  - (g.floor_val + (1 - g.floor_val) * exp(-7.0  / g.tau)), 2)
      + power(c.retention_rate_d30 - (g.floor_val + (1 - g.floor_val) * exp(-30.0 / g.tau)), 2)
      + power(c.retention_rate_d60 - (g.floor_val + (1 - g.floor_val) * exp(-60.0 / g.tau)), 2)
      + power(c.retention_rate_d90 - (g.floor_val + (1 - g.floor_val) * exp(-90.0 / g.tau)), 2)
            as sse
    from cohort_points c
    cross join grid g
),

best_per_cohort as (
    select distinct on (cohort_week)
        cohort_week,
        cohort_size,
        tau,
        floor_val,
        sse
    from candidates
    order by cohort_week, sse asc, floor_val asc
)

select
    cohort_week,
    cohort_size,
    floor_val                                                       as fitted_floor,
    tau                                                             as fitted_tau_days,
    round(sse::numeric, 6)                                          as sse,
    round((floor_val + (1 - floor_val) * exp(-180.0 / tau))::numeric, 4)
        as retention_d180_estimate,
    round((floor_val + (1 - floor_val) * exp(-365.0 / tau))::numeric, 4)
        as retention_d365_estimate
from best_per_cohort
order by cohort_week desc
