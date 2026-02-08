select 
    team_id,
    team_name,
    team_abb,
    league_id
from {{ ref('snp_teams') }}