/*
The percentage of search having clicks per day, over the last 7 days, including
overall summary value (using a single SQL query, adding a column to produce
the summary value for the overall period)
*/

with searches_having_clicks as (
    select
        DATE_TRUNC('day', s."datetime") as "day",
        COUNT(distinct "id") as "searchCount",
        COUNT(distinct case when c."clickId" is not null then "id" end)
            as "clickCount",
        COUNT(distinct case when c."clickId" is not null then "id" end)
        * 100.0
        / COUNT(distinct "id") as "clickPercentage"
    from {{ ref('stg_searches') }} as s
    left join {{ ref('stg_clicks') }} as c on s."id" = c."searchId"
    where
        s."datetime"
        >= (
            select MAX("datetime") - interval '7 days'
            from {{ ref('stg_searches') }}
        )
    group by rollup ("day")
    order by "day"
)

select *
from searches_having_clicks
