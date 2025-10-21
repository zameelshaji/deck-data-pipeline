select 
    user_id, 
    place_id, 
    created_at, 
    date(created_at) as created_date
from {{ source("legacy", "user_liked_places") }}
