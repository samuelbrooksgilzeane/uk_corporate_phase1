{{ config(materialized='view') }}

select
    {{ ch_company_columns() }},
    resource_uri,
    published_at as latest_event_at
from {{ ref('base_ch_latest_records') }}
where stream_name = 'companies'
