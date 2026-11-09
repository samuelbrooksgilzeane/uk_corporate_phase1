select company_number
from {{ ref('dim_ch_company_history') }}
group by company_number
having sum(case when is_latest_version then 1 else 0 end) <> 1
