{{ config(materialized='view') }}

select
    resource_uri as filing_uri,
    nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '') as company_number,
    get_json_object(data_json, '$.transaction_id') as transaction_id,
    to_date(get_json_object(data_json, '$.date')) as filed_on,
    get_json_object(data_json, '$.type') as filing_type,
    get_json_object(data_json, '$.category') as filing_category,
    get_json_object(data_json, '$.description') as filing_description_code,
    get_json_object(data_json, '$.links.document_metadata') as document_uri,
    published_at as latest_event_at
from {{ ref('base_ch_latest_records') }}
where stream_name = 'filings'

