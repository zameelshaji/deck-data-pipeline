select 
    id as user_id,
    email,
    email_confirmed_at
from {{ source("auth", "users") }}
where id::text not in (select id from {{ref('src_test_accounts')}})

