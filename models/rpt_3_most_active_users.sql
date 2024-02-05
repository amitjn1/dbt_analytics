/*
List the 10 most active users (by number of searches) having at least 1 click on a
document coming from a source starting with “Confluence”.
*/

SELECT
    s."userId",
    COUNT(DISTINCT s."id") AS "searchCount",
    SUM(CASE
        WHEN
            c."searchId" IS NOT null
            AND UPPER(c."clickCause") LIKE 'DOCUMENT%'
            AND UPPER(c."sourceName") LIKE 'CONFLUENCE%' THEN 1
        ELSE 0
    END) AS "clickCount"
FROM {{ ref('stg_searches') }} AS s
LEFT JOIN {{ ref('stg_clicks') }} AS c ON s."id" = c."searchId"
GROUP BY s."userId"
HAVING "clickCount" > 0
ORDER BY "searchCount" DESC
LIMIT 10
