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
    case
        when its.points = 3 then 'W'
        when its.points = 1 then 'D'
        else 'L'
    end as result,
    g.game_date,
    g.game_round,
    its.opponent_team_id,
    t.team_name as opponent_name
from {{ ref('int_team_stats') }} its
join {{ ref('fct_games') }} g on its.game_id = g.game_id
left join {{ ref('dim_teams') }} t on its.opponent_team_id = t.team_id