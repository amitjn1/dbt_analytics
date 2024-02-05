-- List all visits with at least one click

select distinct "visitId"
from {{ ref('stg_clicks') }}
