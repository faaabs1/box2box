select
    team_id,
    league_id,
    game_location,
    season_id,
    count(*)             as games_played,
    sum(goals_scored)    as goals_scored,
    sum(goals_conceded)  as goals_conceded,
    sum(goal_difference) as goal_difference,
    sum(points)          as points,
    sum(points_allowed)  as points_allowed
from {{ref('int_team_stats')}}
group by team_id, game_location, season_id, league_id