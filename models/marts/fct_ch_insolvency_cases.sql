{{ config(materialized='view') }}

with exploded as (
    select
        company_number,
        insolvency_uri,
        explode_outer(from_json(
            cases_json,
            'array<struct<number:string,type:string,dates:array<struct<type:string,date:string>>>>'
        )) as case_record,
        latest_event_at
    from {{ ref('stg_ch_insolvency') }}
)

select
    sha2(concat_ws('|', company_number, coalesce(case_record.number, ''),
                   coalesce(case_record.type, '')), 256) as insolvency_case_key,
    company_number,
    insolvency_uri,
    case_record.number as case_number,
    case_record.type as case_type,
    case_record.dates as case_dates,
    latest_event_at
from exploded
where case_record is not null

