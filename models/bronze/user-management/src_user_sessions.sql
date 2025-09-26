select 
    user_id,
    start_time,
    end_time
from 
    {{ source('public', 'user_sessions') }}