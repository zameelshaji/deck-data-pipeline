-- fitted_floor must be a valid retention share in [0, 1].
-- If the grid search ever produces a value outside this range, the grid
-- definition is broken.

select *
from {{ ref('fct_cohort_retention_floor') }}
where fitted_floor < 0 or fitted_floor > 1
