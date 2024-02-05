select *
from {{ source('click_stream', 'custom_events') }}
