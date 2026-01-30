select 
    stg_games.*,
    home_team.team_name as home_team_name,
    away_team.team_name as away_team_name,
    league.league_name as league_name,
    season.season_name as season_name

from {{ ref('stg_games')}}
join {{ ref('stg_teams')}} as home_team on stg_games.home_team_id = home_team.team_id
join {{ ref('stg_teams')}} as away_team on stg_games.away_team_id = away_team.team_id
join {{ source('box2box_raw', 'leagues') }} as league on stg_games.league_id = league.league_id
join {{ source('box2box_raw', 'seasons') }} as season on stg_games.game_date >= season.valid_from AND stg_games.game_date <= season.valid_to