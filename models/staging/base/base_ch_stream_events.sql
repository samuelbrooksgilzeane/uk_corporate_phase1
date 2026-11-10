{{ config(materialized='view') }}

-- The stream timepoint is monotonic *within* one stream, not across all streams.
with ranked as (
    select
        stream_name,
        cast(event_timepoint as bigint) as event_timepoint,
        resource_uri,
        resource_id,
        resource_kind,
        lower(event_type) as event_type,
        to_timestamp(published_at) as published_at,
        data_json,
        ingested_at,
        row_number() over (
            partition by stream_name, event_timepoint
            order by ingested_at desc
        ) as retry_rank
    from {{ source('companies_house', 'ch_stream_events') }}
)

select
    stream_name,
    event_timepoint,
    resource_uri,
    resource_id,
    resource_kind,
    event_type,
    published_at,
    data_json,
    ingested_at,
    sha2(concat(stream_name, ':', cast(event_timepoint as string)), 256) as event_id
from ranked
where retry_rank = 1

