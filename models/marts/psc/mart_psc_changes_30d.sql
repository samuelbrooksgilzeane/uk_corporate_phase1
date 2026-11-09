{{ config(materialized='view') }}

-- An indicator of activity, NOT a misconduct classification.
with versions as (
    select
        *,
        row_number() over (
            partition by psc_id order by valid_from_observed_on
        ) as version_number
    from {{ ref('dim_psc_history_bulk') }}
)

select
    company_number,
    count(*) as psc_record_changes_last_30d
from versions
where version_number > 1
  and valid_from_observed_on >= date_sub(current_date(), 30)
group by company_number
