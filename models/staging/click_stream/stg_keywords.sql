select *
from {{ source('click_stream', 'keywords') }}
