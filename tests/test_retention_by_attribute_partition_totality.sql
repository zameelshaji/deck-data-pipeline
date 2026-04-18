-- For each (cohort_week, attribute_name), the sum of cohort_size across all
-- attribute_values must equal cohort_size in fct_retention_by_cohort_week for
-- the same cohort_week. Each attribute must totally partition the cohort.

with per_attr as (
    select
        cohort_week,
        attribute_name,
        sum(cohort_size) as summed_cohort_size
    from {{ ref('fct_retention_by_acquisition_attribute') }}
    group by cohort_week, attribute_name
),

baseline as (
    select cohort_week, cohort_size
    from {{ ref('fct_retention_by_cohort_week') }}
)

select
    p.cohort_week,
    p.attribute_name,
    p.summed_cohort_size,
    b.cohort_size
from per_attr p
inner join baseline b on p.cohort_week = b.cohort_week
where p.summed_cohort_size <> b.cohort_size
