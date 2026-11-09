{{ config(materialized='table', file_format='delta') }}

select
    company_number,
    company_name,
    company_status,
    company_type,
    jurisdiction,
    incorporated_on,
    dissolved_on,
    registered_office_postcode,
    sic_codes,
    latest_event_at
from {{ ref('stg_ch_companies') }}
where company_number is not null

