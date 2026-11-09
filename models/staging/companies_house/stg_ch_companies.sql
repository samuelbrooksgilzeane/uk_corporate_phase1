{{ config(materialized='view') }}

select
    coalesce(
        get_json_object(data_json, '$.company_number'),
        nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), '')
    ) as company_number,
    get_json_object(data_json, '$.company_name') as company_name,
    get_json_object(data_json, '$.company_status') as company_status,
    get_json_object(data_json, '$.type') as company_type,
    get_json_object(data_json, '$.jurisdiction') as jurisdiction,
    to_date(get_json_object(data_json, '$.date_of_creation')) as incorporated_on,
    to_date(get_json_object(data_json, '$.date_of_cessation')) as dissolved_on,
    get_json_object(data_json, '$.registered_office_address.postal_code') as registered_office_postcode,
    from_json(get_json_object(data_json, '$.sic_codes'), 'array<string>') as sic_codes,
    get_json_object(data_json, '$.previous_company_names') as previous_names_json,
    resource_uri,
    published_at as latest_event_at
from {{ ref('int_ch_latest_records') }}
where stream_name = 'companies'

