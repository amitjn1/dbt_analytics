/*
List the top 10 most popular items and display their average click rank.
*/

SELECT
    "documentTitle",
    COUNT(DISTINCT "visitId") AS "clickCount",
    AVG("clickRank") AS "avgClickRank"
FROM {{ ref('stg_clicks') }}
GROUP BY 1
ORDER BY
    "clickCount" DESC
LIMIT 10
