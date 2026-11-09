{{ config(materialized='view') }}

select
    snapshot_date,
    part_number,
    psc_uri,
    company_number,
    data_json,
    loaded_at
from {{ source('companies_house', 'psc_bulk_snapshots') }}
