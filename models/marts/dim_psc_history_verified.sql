{{ config(
    enabled=var('enable_psc_stream_union', false),
    materialized='table',
    file_format='delta'
) }}

-- This model is intentionally DISABLED until a matching PSC snapshot-to-stream
-- timepoint has been established and checked. Never guess the cutover.
{% if var('enable_psc_stream_union', false) %}
    {% if var('psc_baseline_date', none) is none or var('psc_stream_cutover_timepoint', none) is none %}
        {{ exceptions.raise_compiler_error(
            'Set psc_baseline_date and verified psc_stream_cutover_timepoint before enabling this model.'
        ) }}
    {% endif %}
{% endif %}

with baseline as (
    select
        psc_id,
        company_number,
        psc_name,
        psc_kind,
        notified_on,
        ceased_on,
        natures_of_control,
        source_etag,
        payload_fingerprint,
        cast(0 as bigint) as event_timepoint,
        to_timestamp(cast(snapshot_date as string)) as event_timestamp,
        false as is_deleted,
        'bulk_baseline' as source_kind
    from {{ ref('stg_psc_bulk') }}
    where snapshot_date = date('{{ var("psc_baseline_date", "1970-01-01") }}')
), stream as (
    select
        resource_uri as psc_id,
        nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
        get_json_object(data_json, '$.name') as psc_name,
        get_json_object(data_json, '$.kind') as psc_kind,
        to_date(get_json_object(data_json, '$.notified_on')) as notified_on,
        to_date(get_json_object(data_json, '$.ceased_on')) as ceased_on,
        from_json(
            get_json_object(data_json, '$.natures_of_control'), 'array<string>'
        ) as natures_of_control,
        get_json_object(data_json, '$.etag') as source_etag,
        coalesce(
            get_json_object(data_json, '$.etag'), sha2(coalesce(data_json, ''), 256)
        ) as payload_fingerprint,
        event_timepoint,
        published_at as event_timestamp,
        event_type = 'deleted' as is_deleted,
        'psc_stream' as source_kind
    from {{ ref('stg_ch_stream_events') }}
    where stream_name = 'psc'
      and resource_uri like '%/persons-with-significant-control/%'
      and event_timepoint > {{ var('psc_stream_cutover_timepoint', 0) }}
), combined as (
    select * from baseline
    union all
    select * from stream
), ordered as (
    select
        *,
        lag(payload_fingerprint) over (
            partition by psc_id order by event_timepoint
        ) as prior_fingerprint,
        lag(is_deleted) over (
            partition by psc_id order by event_timepoint
        ) as prior_deleted,
        row_number() over (
            partition by psc_id order by event_timepoint
        ) as event_rank
    from combined
), changes as (
    select *
    from ordered
    where event_rank = 1
       or not (payload_fingerprint <=> prior_fingerprint)
       or not (is_deleted <=> prior_deleted)
), ranged as (
    select
        *,
        lead(event_timepoint) over (
            partition by psc_id order by event_timepoint
        ) as valid_to_timepoint,
        lead(event_timestamp) over (
            partition by psc_id order by event_timepoint
        ) as valid_to_observed_at
    from changes
)

select
    sha2(concat(psc_id, ':', cast(event_timepoint as string)), 256) as psc_version_id,
    psc_id,
    company_number,
    psc_name,
    psc_kind,
    notified_on,
    ceased_on,
    natures_of_control,
    source_etag,
    source_kind,
    is_deleted,
    event_timepoint as valid_from_timepoint,
    valid_to_timepoint,
    event_timestamp as valid_from_observed_at,
    valid_to_observed_at,
    valid_to_timepoint is null as is_latest_version
from ranged
