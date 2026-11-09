{{ config(materialized='table', file_format='delta') }}

-- Compact SCD2 over the *incremental* daily differences table.
-- A missing record is recorded as an observation, not a legal deletion.
with ordered as (
    select
        *,
        lead(snapshot_date) over (
            partition by psc_id order by snapshot_date
        ) as valid_to_observed_on
    from {{ ref('int_psc_daily_deltas') }}
)

select
    psc_change_id as psc_version_id,
    psc_id,
    company_number,
    psc_name,
    psc_kind,
    corporate_registration_number,
    corporate_registration_country,
    birth_year,
    birth_month,
    natures_of_control,
    notified_on,
    ceased_on,
    source_etag,
    observation_type,
    is_missing_from_snapshot,
    snapshot_date as valid_from_observed_on,
    valid_to_observed_on,
    valid_to_observed_on is null as is_latest_observation
from ordered
