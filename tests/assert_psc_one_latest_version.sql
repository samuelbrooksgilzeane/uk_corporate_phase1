-- A singular dbt test fails if this SELECT returns any rows.
select psc_id
from {{ ref('dim_psc_history') }}
group by psc_id
having sum(case when is_latest_version then 1 else 0 end) <> 1

