-- Event order must be monotonic; observed timestamps should not move backwards.
select *
from {{ ref('dim_psc_history') }}
where (
    valid_to_timepoint is not null
    and valid_to_timepoint <= valid_from_timepoint
)
or (
    valid_to_observed_at is not null
    and valid_to_observed_at < valid_from_observed_at
)

