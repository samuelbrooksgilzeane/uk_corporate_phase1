{{ config(materialized='view') }}

select
    resource_uri as statement_uri,
    nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
    get_json_object(data_json, '$.statement') as statement_code,
    to_date(get_json_object(data_json, '$.notified_on')) as notified_on,
    to_date(get_json_object(data_json, '$.ceased_on')) as ceased_on,
    published_at as latest_event_at
from {{ ref('int_ch_latest_records') }}
where stream_name = 'psc_statements'

