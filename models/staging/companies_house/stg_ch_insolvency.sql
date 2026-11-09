{{ config(materialized='view') }}

select
    resource_uri as insolvency_uri,
    nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
    get_json_object(data_json, '$.cases') as cases_json,
    published_at as latest_event_at
from {{ ref('base_ch_latest_records') }}
where stream_name = 'insolvency'

