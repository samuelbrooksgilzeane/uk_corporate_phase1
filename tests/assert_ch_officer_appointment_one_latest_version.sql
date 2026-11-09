select appointment_uri
from {{ ref('dim_ch_officer_appointment_history') }}
group by appointment_uri
having sum(case when is_latest_version then 1 else 0 end) <> 1
