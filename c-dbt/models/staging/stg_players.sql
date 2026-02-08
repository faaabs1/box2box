select 
        player_id,
        firstname,
        lastname,
        position1,
        position2,
        strong_foot,
        jersey_number,
        dob,
        team_id
from {{ ref('snp_players') }}