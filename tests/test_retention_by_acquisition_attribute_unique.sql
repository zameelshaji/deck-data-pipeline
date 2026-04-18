-- Uniqueness of (cohort_week, attribute_name, attribute_value) in
-- fct_retention_by_acquisition_attribute. A duplicate row would mean a
-- user appeared twice in the same (cohort, attribute) slice.

select
    cohort_week,
    attribute_name,
    attribute_value,
    count(*) as duplicate_row_count
from {{ ref('fct_retention_by_acquisition_attribute') }}
group by cohort_week, attribute_name, attribute_value
having count(*) > 1
