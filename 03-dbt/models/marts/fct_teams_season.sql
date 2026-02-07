select
    team_id,
    location,
    league_id,
    sum(goals_scored) as total_goals_scored,
    sum(goals_conceded) as total_goals_conceded,
    sum(goal_difference) as total_goal_difference,
    sum(points) as total_points,
    sum(points_allowed) as total_points_allowed,
    season_id
from {{ref('int_team_stats')}}
group by team_id, season_id