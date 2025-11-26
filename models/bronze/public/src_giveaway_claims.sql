select * 
from {{ source("public", "giveaway_claims") }}
