-- Sum of cohort_size across connectivity buckets per cohort_week must equal
-- cohort_size in fct_retention_by_cohort_week. Each week is fully partitioned
-- into 0_groups / 1-2_groups / 3+_groups.

with per_week as (
    select cohort_week, sum(cohort_size) as summed_cohort_size
    from {{ ref('fct_retention_by_social_connectivity') }}
    group by cohort_week
),

baseline as (
    select cohort_week, cohort_size
    from {{ ref('fct_retention_by_cohort_week') }}
)

select
    p.cohort_week,
    p.summed_cohort_size,
    b.cohort_size
from per_week p
inner join baseline b on p.cohort_week = b.cohort_week
where p.summed_cohort_size <> b.cohort_size
