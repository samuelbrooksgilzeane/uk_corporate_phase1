{{ config(materialized='view') }}

-- A snapshot only counts once every ZIP part has been committed. The previous
-- date skips incomplete snapshots, so each date is compared with the last
-- complete one.
select
    snapshot_date,
    lag(snapshot_date) over (order by snapshot_date) as previous_snapshot_date
from {{ source('companies_house', 'psc_bulk_manifest') }}
where is_complete = true
