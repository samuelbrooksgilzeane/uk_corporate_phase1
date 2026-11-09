{{ config(materialized='table', file_format='delta') }}

-- Event-sourced SCD2 over every officers stream event. Consecutive events
-- with the same payload collapse into one version; deletions are kept.
with ordered as (
    select
        *,
        lag(payload_fingerprint) over (
            partition by appointment_uri order by event_timepoint
        ) as prior_fingerprint,
        lag(is_deleted) over (
            partition by appointment_uri order by event_timepoint
        ) as prior_deleted,
        row_number() over (
            partition by appointment_uri order by event_timepoint
        ) as event_rank
    from {{ ref('stg_ch_officer_appointment_events') }}
    where appointment_uri is not null
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
            partition by appointment_uri order by event_timepoint
        ) as valid_to_timepoint,
        lead(published_at) over (
            partition by appointment_uri order by event_timepoint
        ) as valid_to_observed_at
    from changes
)

select
    sha2(concat(appointment_uri, ':', cast(event_timepoint as string)), 256) as appointment_version_id,
    appointment_uri,
    company_number,
    officer_id,
    officer_name,
    officer_role,
    appointed_on,
    resigned_on,
    birth_year,
    birth_month,
    event_type,
    is_deleted,
    event_timepoint as valid_from_timepoint,
    valid_to_timepoint,
    published_at as valid_from_observed_at,
    valid_to_observed_at,
    valid_to_timepoint is null as is_latest_version
from ranged
