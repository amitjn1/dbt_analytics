/*
List all events related to visits, in order of date, adding the following information
○ The sequence number (1 to N) of the event over the visit
○ The time difference (in milliseconds) between the event and the previous one. The first event having 0 or null since no previous event
*/

with all_events as (
    select
        "visitId",
        "id" as "searchId",
        "datetime",
        'Search event' as "eventSrcTable"
    from {{ ref('stg_searches') }}
    union all
    select
        "visitId",
        "searchId",
        "datetime",
        'Click event'
    from {{ ref('stg_clicks') }}
    union all
    select
        "visitId",
        "lastSearchId",
        "datetime",
        'Custom event'
    from {{ ref('stg_custom_events') }}
),

ranked_events as (
    select
        v.*,
        ROW_NUMBER()
            over (partition by v."visitId" order by v."datetime")
            as "sequenceNumber",
        LAG(v."datetime")
            over (partition by v."visitId" order by v."datetime")
            as "previousEventTime"
    from
        all_events as v
    order by
        v."visitId", "sequenceNumber"
)

select
    *,
    COALESCE(DATEDIFF('millisecond', "previousEventTime", "datetime"), 0)
        as "timeDifference"
from
    ranked_events
