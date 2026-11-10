{{ config(materialized='view') }}

-- PSC stream changes are intentionally kept separate from bulk history until
-- the snapshot/stream cutover has been reconciled.
select
    resource_uri as psc_id,
    nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
    event_timepoint,
    event_type,
    get_json_object(data_json, '$.name') as psc_name,
    get_json_object(data_json, '$.kind') as psc_kind,
    get_json_object(data_json, '$.etag') as source_etag,
    to_date(get_json_object(data_json, '$.notified_on')) as notified_on,
    to_date(get_json_object(data_json, '$.ceased_on')) as ceased_on,
    from_json(
        get_json_object(data_json, '$.natures_of_control'), 'array<string>'
    ) as natures_of_control,
    coalesce(
        get_json_object(data_json, '$.etag'), sha2(coalesce(data_json, ''), 256)
    ) as payload_fingerprint,
    published_at
from {{ ref('base_ch_stream_events') }}
where stream_name = 'psc'
