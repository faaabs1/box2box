select
    its.game_id,
    its.team_id,
    its.season_id,
    its.league_id,
    its.game_location,
    its.goals_scored,
    its.goals_conceded,
    its.goal_difference,
    its.points,
    g.game_date,
    g.game_round,
    its.opponent_team_id
from {{ ref('int_team_stats') }} its
join {{ ref('fct_games') }} g on its.game_id = g.game_id