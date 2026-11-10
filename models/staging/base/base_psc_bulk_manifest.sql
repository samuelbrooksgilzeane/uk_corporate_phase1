{{ config(materialized='view') }}

select
    snapshot_date,
    expected_parts,
    loaded_parts,
    is_complete,
    recorded_at
from {{ source('companies_house', 'psc_bulk_manifest') }}
