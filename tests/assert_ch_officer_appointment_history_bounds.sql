select *
from {{ ref('dim_ch_officer_appointment_history') }}
where valid_to_timepoint is not null
  and valid_to_timepoint <= valid_from_timepoint
