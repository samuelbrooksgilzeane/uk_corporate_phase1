{{ config(materialized='view') }}

with deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by psc_id, event_timepoint
                order by ingested_at desc
            ) as ingest_rank
        from {{ ref('stg_psc_events') }}
    ) x
    where ingest_rank = 1
),

previous_state as (
    select
        *,
        lag(payload_fingerprint) over (
            partition by psc_id order by event_timepoint
        ) as previous_fingerprint,
        lag(is_deleted) over (
            partition by psc_id order by event_timepoint
        ) as previous_deleted,
        row_number() over (
            partition by psc_id order by event_timepoint
        ) as event_order
    from deduplicated
)

select
    psc_id,
    company_number,
    event_timepoint,
    event_timestamp,
    event_type,
    is_deleted,
    psc_name,
    psc_kind,
    notified_on,
    ceased_on,
    birth_year,
    birth_month,
    natures_of_control,
    source_etag,
    payload_fingerprint
from previous_state
where event_order = 1
    or not (payload_fingerprint <=> previous_fingerprint)
    or not (is_deleted <=> previous_deleted)

