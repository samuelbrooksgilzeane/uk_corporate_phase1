{{ config(materialized='view') }}

-- Every officers stream event, including deletions, for building history.
select
    resource_uri as appointment_uri,
    {{ ch_officer_appointment_columns() }},
    event_timepoint,
    event_type,
    event_type = 'deleted' as is_deleted,
    coalesce(
        get_json_object(data_json, '$.etag'), sha2(coalesce(data_json, ''), 256)
    ) as payload_fingerprint,
    published_at
from {{ ref('base_ch_stream_events') }}
where stream_name = 'officers'
