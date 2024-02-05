select *
from {{ source('click_stream', 'clicks') }}
