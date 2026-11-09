select *
from {{ ref('dim_psc_history_bulk') }}
where valid_to_observed_on is not null
  and valid_to_observed_on <= valid_from_observed_on

