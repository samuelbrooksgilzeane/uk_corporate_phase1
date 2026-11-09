select psc_id
from {{ ref('dim_psc_history_bulk') }}
group by psc_id
having sum(case when is_latest_observation then 1 else 0 end) <> 1

