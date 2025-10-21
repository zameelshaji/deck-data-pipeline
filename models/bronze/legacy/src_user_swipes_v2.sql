select 
    user_id, 
    place_id, 
    direction, 
    created_at, 
    date(created_at) as created_date
from {{ source("legacy", "user_swipes_ver2") }}
