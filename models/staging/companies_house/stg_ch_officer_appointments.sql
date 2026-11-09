{{ config(materialized='view') }}

select
    resource_uri as appointment_uri,
    {{ ch_officer_appointment_columns() }},
    latest_event_at
from (
    select *, published_at as latest_event_at
    from {{ ref('base_ch_latest_records') }}
    where stream_name = 'officers'
)
