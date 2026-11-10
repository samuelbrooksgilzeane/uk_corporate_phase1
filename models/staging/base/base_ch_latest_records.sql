{{ config(materialized='view') }}

-- Latest record per source URI. A deletion removes it from the *current* view,
-- while every version remains available in base_ch_stream_events.
with ranked as (
    select
        *,
        row_number() over (
            partition by stream_name, coalesce(resource_uri, resource_id)
            order by event_timepoint desc
        ) as current_rank
    from {{ ref('base_ch_stream_events') }}
    where resource_uri is not null or resource_id is not null
)

select * except (current_rank)
from ranked
where current_rank = 1 and event_type <> 'deleted'

