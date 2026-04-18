select 
    g.goal_id,
    g.game_id,
    g.season_id,
    g.goal_min,
    g.player_id,
    g.game_situation,
    g.own_goal,
    g.goal_for_team_id,
    g.goal_time_bucket,
    g.goal_half,

    {{ game_location('g.goal_for_team_id', 'ga.home_team_id') }} AS game_location,

    {{ opponent_team_id('g.goal_for_team_id', 'ga.home_team_id', 'ga.away_team_id') }} as goal_against_team_id

from {{ref('stg_goals')}} g 
join {{ref('fct_games')}} ga on g.game_id = ga.game_id