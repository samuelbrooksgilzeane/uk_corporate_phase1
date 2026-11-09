{{ config(materialized='table', file_format='delta') }}

-- Event-sourced SCD2: one row per observed PSC state, including explicit deletes.
-- Rebuilding preserves every stored event, but daily polling cannot see all
-- intermediate changes that happen between polls.

with versions as (
    select
        *,
        lead(event_timestamp) over (
            partition by psc_id order by event_timepoint
        ) as valid_to_observed_at,
        lead(event_timepoint) over (
            partition by psc_id order by event_timepoint
        ) as valid_to_timepoint
    from {{ ref('int_psc_changes') }}
)

select
    sha2(concat(psc_id, ':', cast(event_timepoint as string)), 256) as psc_version_id,
    psc_id,
    company_number,
    psc_name,
    psc_kind,
    notified_on,
    ceased_on,
    birth_year,
    birth_month,
    natures_of_control,
    source_etag,
    event_type,
    is_deleted,
    event_timepoint as valid_from_timepoint,
    valid_to_timepoint,
    event_timestamp as valid_from_observed_at,
    valid_to_observed_at,
    valid_to_timepoint is null as is_latest_version
from versions

