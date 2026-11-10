{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='psc_change_id',
    file_format='delta'
) }}

-- For each newly completed snapshot date, compare it to the PREVIOUS complete
-- snapshot date. A missing record is flagged, not assumed legally deleted.
with dates_to_process as (
    select snapshot_date, previous_snapshot_date
    from {{ ref('base_psc_bulk_complete_snapshots') }}
    {% if is_incremental() %}
    where snapshot_date > (
        select coalesce(max(snapshot_date), date('1900-01-01')) from {{ this }}
    )
    {% endif %}
), current_records as (
    select p.*, d.previous_snapshot_date
    from {{ ref('stg_psc_bulk') }} p
    inner join dates_to_process d on p.snapshot_date = d.snapshot_date
), prior_records as (
    select
        d.snapshot_date as comparison_date,
        p.*
    from {{ ref('stg_psc_bulk') }} p
    inner join dates_to_process d on p.snapshot_date = d.previous_snapshot_date
), changed as (
    select
        c.snapshot_date,
        c.psc_id,
        c.company_number,
        c.psc_name,
        c.psc_kind,
        c.corporate_registration_number,
        c.corporate_registration_country,
        c.birth_year,
        c.birth_month,
        c.natures_of_control,
        c.notified_on,
        c.ceased_on,
        c.source_etag,
        c.payload_fingerprint,
        case
            when c.previous_snapshot_date is null then 'baseline'
            when p.psc_id is null then 'appeared'
            else 'changed'
        end as observation_type,
        false as is_missing_from_snapshot
    from current_records c
    left join prior_records p
        on c.psc_id = p.psc_id and c.snapshot_date = p.comparison_date
    where p.psc_id is null
       or not (c.payload_fingerprint <=> p.payload_fingerprint)
), missing as (
    select
        p.comparison_date as snapshot_date,
        p.psc_id,
        p.company_number,
        p.psc_name,
        p.psc_kind,
        p.corporate_registration_number,
        p.corporate_registration_country,
        p.birth_year,
        p.birth_month,
        p.natures_of_control,
        p.notified_on,
        p.ceased_on,
        p.source_etag,
        -- No payload: the record wasn't in this snapshot.
        cast(null as string) as payload_fingerprint,
        'missing' as observation_type,
        true as is_missing_from_snapshot
    from prior_records p
    left join current_records c
        on p.psc_id = c.psc_id and p.comparison_date = c.snapshot_date
    where c.psc_id is null
), combined as (
    select * from changed
    union all
    select * from missing
)

select
    sha2(concat(psc_id, ':', cast(snapshot_date as string)), 256) as psc_change_id,
    *
from combined
