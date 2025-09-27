select 
    query_id,
    user_id,
    query_text,
    query_timestamp,
    response_pack_id,
    processing_time,
    query_context::jsonb ->> 'location' as location,
    query_context::jsonb ->> 'app_version' as app_version
from
    {{ source('public', 'dextr_queries') }}