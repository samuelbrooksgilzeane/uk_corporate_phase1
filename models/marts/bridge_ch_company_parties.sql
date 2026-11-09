{{ config(materialized='view') }}

-- Record-level edges only. PSC records are NOT global person identifiers.
with psc as (
    select
        company_number,
        'psc' as relationship_type,
        psc_id as relationship_record_id,
        psc_name as party_name,
        notified_on as relationship_start,
        ceased_on as relationship_end,
        psc_kind as role_detail
    from {{ ref('dim_psc_history_bulk') }}
    where is_latest_observation = true
      and psc_id in (
          select psc_id
          from {{ ref('stg_psc_bulk') }}
          where snapshot_date = (
              select max(snapshot_date) from {{ ref('stg_psc_bulk') }}
          )
      )
), officers as (
    select
        company_number,
        'officer' as relationship_type,
        appointment_uri as relationship_record_id,
        officer_name as party_name,
        appointed_on as relationship_start,
        resigned_on as relationship_end,
        officer_role as role_detail
    from {{ ref('stg_ch_officer_appointments') }}
)

select * from psc
union all
select * from officers

