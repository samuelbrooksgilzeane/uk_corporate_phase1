{{ config(materialized='view') }}

select
    resource_uri as charge_uri,
    nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
    get_json_object(data_json, '$.id') as charge_id,
    get_json_object(data_json, '$.status') as charge_status,
    to_date(get_json_object(data_json, '$.created_on')) as created_on,
    to_date(get_json_object(data_json, '$.delivered_on')) as delivered_on,
    to_date(get_json_object(data_json, '$.satisfied_on')) as satisfied_on,
    get_json_object(data_json, '$.persons_entitled') as persons_entitled_json,
    published_at as latest_event_at
from {{ ref('int_ch_latest_records') }}
where stream_name = 'charges'

