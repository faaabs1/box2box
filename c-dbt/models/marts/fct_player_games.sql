select
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.team_id,
    li.season_id,
    li.game_location,
    li.opponent_team_id,
    t.team_name             as opponent_name,
    li.started,
    li.min_played,
    li.sub_in,
    li.sub_out,
    ga.game_date,
    ga.game_round,
    tg.league_id,
    tg.goals_scored,
    tg.goals_conceded,
    tg.result,
    coalesce(count(fg.goal_id), 0) as goals_by_player

from {{ ref('fct_lineups') }} li
join {{ ref('fct_games') }} ga
    on li.game_id = ga.game_id
join {{ ref('fct_team_games') }} tg
    on tg.game_id = li.game_id and tg.team_id = li.team_id
left join {{ ref('dim_teams') }} t
    on li.opponent_team_id = t.team_id
left join {{ ref('fct_goals') }} fg
    on  fg.game_id         = li.game_id
    and fg.player_id       = li.player_id
    and fg.own_goal        = false

group by
    li.lineup_id, li.game_id, li.player_id, li.team_id, li.season_id,
    li.game_location, li.opponent_team_id, t.team_name,
    li.started, li.min_played, li.sub_in, li.sub_out,
    ga.game_date, ga.game_round,
    tg.league_id, tg.goals_scored, tg.goals_conceded, tg.result
