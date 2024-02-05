select *
from {{ source('click_stream', 'groups') }}
