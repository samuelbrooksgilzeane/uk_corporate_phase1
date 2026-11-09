{{ config(materialized='view') }}

-- Latest observed records which Companies House has not marked as ceased/deleted.
select
    psc_id,
    company_number,
    psc_name,
    psc_kind,
    notified_on,
    natures_of_control,
    valid_from_observed_at
from {{ ref('dim_psc_history') }}
where is_latest_version = true
  and is_deleted = false
  and ceased_on is null

