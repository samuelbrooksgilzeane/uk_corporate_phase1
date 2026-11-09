{{ config(materialized='table', file_format='delta') }}

-- Event-sourced SCD2 over every companies stream event. Consecutive events
-- with the same payload collapse into one version; deletions are kept.
with ordered as (
    select
        *,
        lag(payload_fingerprint) over (
            partition by company_number order by event_timepoint
        ) as prior_fingerprint,
        lag(is_deleted) over (
            partition by company_number order by event_timepoint
        ) as prior_deleted,
        row_number() over (
            partition by company_number order by event_timepoint
        ) as event_rank
    from {{ ref('stg_ch_company_events') }}
    where company_number is not null
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
            partition by company_number order by event_timepoint
        ) as valid_to_timepoint,
        lead(published_at) over (
            partition by company_number order by event_timepoint
        ) as valid_to_observed_at
    from changes
)

select
    sha2(concat(company_number, ':', cast(event_timepoint as string)), 256) as company_version_id,
    company_number,
    company_name,
    company_status,
    company_type,
    jurisdiction,
    incorporated_on,
    dissolved_on,
    registered_office_postcode,
    sic_codes,
    previous_names_json,
    event_type,
    is_deleted,
    event_timepoint as valid_from_timepoint,
    valid_to_timepoint,
    published_at as valid_from_observed_at,
    valid_to_observed_at,
    valid_to_timepoint is null as is_latest_version
from ranged
