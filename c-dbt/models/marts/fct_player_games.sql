select
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.team_id,
    li.started,
    li.min_played,
    li.sub_in,
    li.sub_out,
    li.game_location,
    li.opponent_team_id,

    ga.game_date,
    ga.game_round,
    ga.season_id,
    ga.home_team_league_id as league_id,

    case
        when li.game_location = 'home' then ga.home_goals
        else ga.away_goals
    end as goals_scored,

    case
        when li.game_location = 'home' then ga.away_goals
        else ga.home_goals
    end as goals_conceded,

    count(case when g.own_goal is false or g.own_goal = 0 then g.goal_id end) as goals_by_player,

    case
        when (case when li.game_location = 'home' then ga.home_goals else ga.away_goals end)
           > (case when li.game_location = 'home' then ga.away_goals else ga.home_goals end) then 'W'
        when (case when li.game_location = 'home' then ga.home_goals else ga.away_goals end)
           = (case when li.game_location = 'home' then ga.away_goals else ga.home_goals end) then 'D'
        else 'L'
    end as result

from {{ ref('fct_lineups') }} li
join {{ ref('fct_games') }} ga
    on li.game_id = ga.game_id
left join {{ ref('fct_goals') }} g
    on  g.game_id   = li.game_id
    and g.player_id = li.player_id

group by
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.team_id,
    li.started,
    li.min_played,
    li.sub_in,
    li.sub_out,
    li.game_location,
    li.opponent_team_id,
    ga.game_date,
    ga.game_round,
    ga.season_id,
    ga.home_team_league_id,
    ga.home_goals,
    ga.away_goals
