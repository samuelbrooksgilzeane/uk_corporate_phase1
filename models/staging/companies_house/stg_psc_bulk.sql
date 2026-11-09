{{ config(materialized='view') }}

-- Only publish dates whose full set of ZIP parts was committed.
with complete_dates as (
    select snapshot_date
    from {{ source('companies_house', 'psc_bulk_manifest') }}
    where is_complete = true
),
raw as (
    select
        b.*,
        row_number() over (
            partition by b.snapshot_date, b.psc_uri
            order by b.loaded_at desc, b.part_number desc
        ) as duplicate_rank
    from {{ source('companies_house', 'psc_bulk_snapshots') }} b
    inner join complete_dates d
        on b.snapshot_date = d.snapshot_date
    where b.psc_uri is not null
)

select
    snapshot_date,
    psc_uri as psc_id,
    company_number,
    get_json_object(data_json, '$.name') as psc_name,
    get_json_object(data_json, '$.kind') as psc_kind,
    get_json_object(data_json, '$.identification.registration_number') as corporate_registration_number,
    get_json_object(data_json, '$.identification.country_registered') as corporate_registration_country,
    to_date(get_json_object(data_json, '$.notified_on')) as notified_on,
    to_date(get_json_object(data_json, '$.ceased_on')) as ceased_on,
    cast(get_json_object(data_json, '$.date_of_birth.year') as int) as birth_year,
    cast(get_json_object(data_json, '$.date_of_birth.month') as int) as birth_month,
    from_json(
        get_json_object(data_json, '$.natures_of_control'), 'array<string>'
    ) as natures_of_control,
    get_json_object(data_json, '$.etag') as source_etag,
    coalesce(get_json_object(data_json, '$.etag'), sha2(data_json, 256)) as payload_fingerprint
from raw
where duplicate_rank = 1

