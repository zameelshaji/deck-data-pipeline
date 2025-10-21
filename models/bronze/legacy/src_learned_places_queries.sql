with source as (
    select 
        query_context,
        min(created_at) as created_at
    from {{ source("legacy", "learned_places") }}
    group by 1
)

select 
    created_at,
    query_context::jsonb ->> 'query' as query
from source
where query_context::jsonb ->> 'query' is not null


