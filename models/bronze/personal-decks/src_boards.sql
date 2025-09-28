select id, user_id, name, description, is_default, created_at, updated_at, is_private
from {{ source("public", "boards") }}
