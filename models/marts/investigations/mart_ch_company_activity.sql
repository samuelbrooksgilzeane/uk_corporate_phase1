{{ config(materialized='table', file_format='delta') }}

with all_companies as (
    select company_number from {{ ref('stg_ch_companies') }}
    union
    select company_number from {{ ref('bridge_ch_company_parties') }}
    union
    select company_number from {{ ref('stg_ch_charges') }}
    union
    select company_number from {{ ref('stg_ch_filings') }}
    union
    select company_number from {{ ref('fct_ch_insolvency_cases') }}
), psc as (
    select company_number, count(*) as n_psc_records
    from {{ ref('bridge_ch_company_parties') }}
    where relationship_type = 'psc' and relationship_end is null
    group by company_number
), officers as (
    select company_number, count(*) as n_officer_records
    from {{ ref('bridge_ch_company_parties') }}
    where relationship_type = 'officer' and relationship_end is null
    group by company_number
), charges as (
    select company_number, count(*) as n_charge_records
    from {{ ref('stg_ch_charges') }}
    group by company_number
), filings as (
    select company_number, count(*) as n_filing_records
    from {{ ref('stg_ch_filings') }}
    group by company_number
), insolvency as (
    select company_number, count(*) as n_insolvency_cases
    from {{ ref('fct_ch_insolvency_cases') }}
    group by company_number
)

select
    c.company_number,
    d.company_name,
    d.company_status,
    coalesce(p.n_psc_records, 0) as n_reported_active_psc_records,
    coalesce(o.n_officer_records, 0) as n_observed_active_officer_records,
    coalesce(ch.n_charge_records, 0) as n_observed_charge_records,
    coalesce(f.n_filing_records, 0) as n_observed_filing_records,
    coalesce(i.n_insolvency_cases, 0) as n_observed_insolvency_cases,
    'partial_since_stream_start' as non_psc_history_coverage
from all_companies c
left join {{ ref('dim_ch_companies') }} d on c.company_number = d.company_number
left join psc p on c.company_number = p.company_number
left join officers o on c.company_number = o.company_number
left join charges ch on c.company_number = ch.company_number
left join filings f on c.company_number = f.company_number
left join insolvency i on c.company_number = i.company_number
where c.company_number is not null

