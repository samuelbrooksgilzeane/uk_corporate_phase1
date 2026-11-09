-- Run once in Databricks SQL, after ensuring the catalog exists.
create schema if not exists uk_corporate.bronze;

create table if not exists uk_corporate.bronze.psc_events (
    psc_uri string,
    company_number string,
    event_type string,
    event_timepoint bigint,
    event_timestamp timestamp,
    ingested_at timestamp,
    payload_json string
) using delta;

