{{ config(materialized='view') }}

-- The ingestion job writes the PSC's inner `data` object to payload_json.
-- A deleted event can have a NULL payload_json.

select
    cast(psc_uri as string) as psc_id,
    cast(company_number as string) as company_number,
    cast(event_timepoint as bigint) as event_timepoint,
    cast(event_timestamp as timestamp) as event_timestamp,
    cast(ingested_at as timestamp) as ingested_at,
    lower(trim(event_type)) as event_type,
    lower(trim(event_type)) = 'deleted' as is_deleted,

    get_json_object(payload_json, '$.name') as psc_name,
    get_json_object(payload_json, '$.kind') as psc_kind,
    to_date(get_json_object(payload_json, '$.notified_on')) as notified_on,
    to_date(get_json_object(payload_json, '$.ceased_on')) as ceased_on,
    cast(get_json_object(payload_json, '$.date_of_birth.year') as int) as birth_year,
    cast(get_json_object(payload_json, '$.date_of_birth.month') as int) as birth_month,
    from_json(
        get_json_object(payload_json, '$.natures_of_control'),
        'array<string>'
    ) as natures_of_control,
    get_json_object(payload_json, '$.etag') as source_etag,

    -- The polling loader serializes JSON with sorted keys, so this hash
    -- detects any observed source-field change rather than relying on etag.
    sha2(coalesce(payload_json, ''), 256) as payload_fingerprint

from {{ source('companies_house', 'psc_events') }}
where psc_uri like '%/persons-with-significant-control/%'

