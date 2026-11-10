{{ config(materialized='view') }}

-- Every companies stream event, including deletions, for building history.
select
    {{ ch_company_columns() }},
    resource_uri,
    event_timepoint,
    event_type,
    event_type = 'deleted' as is_deleted,
    coalesce(
        get_json_object(data_json, '$.etag'), sha2(coalesce(data_json, ''), 256)
    ) as payload_fingerprint,
    published_at
from {{ ref('base_ch_stream_events') }}
where stream_name = 'companies'
