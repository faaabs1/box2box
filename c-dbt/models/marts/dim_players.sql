select 
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_key,
    *
from {{ ref('stg_players') }}