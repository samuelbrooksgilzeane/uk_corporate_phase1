{{ config(materialized='view') }}

-- These records are persons; company links must be verified before joining.
select
    resource_uri as disqualification_uri,
    resource_id as disqualification_id,
    get_json_object(data_json, '$.forename') as forename,
    get_json_object(data_json, '$.surname') as surname,
    get_json_object(data_json, '$.disqualifications') as disqualifications_json,
    published_at as latest_event_at
from {{ ref('base_ch_latest_records') }}
where stream_name = 'disqualifications'

