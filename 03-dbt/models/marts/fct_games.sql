select 
    g.*,
    t.league_id as home_team_league_id,
    t2.league_id as away_team_league_id
from {{ ref('stg_games')}} g
join {{ ref('stg_seasons')}} s 
    on g.game_date <= s.valid_to and g.game_date >= s.valid_from
join {{ ref('stg_teams')}} t
    on g.home_team_id = t.team_id
join {{ ref('stg_teams')}} t2
    on g.away_team_id = t2.team_id