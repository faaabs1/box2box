select 
    g.goal_id,
    g.game_id,
    g.goal_min,
    g.player_id,
    g.game_situation,
    g.own_goal,
    g.goal_for_team_id,
    ga.season_id,

    --4. Goal Location (Home/Away)
    CASE 
        when g.goal_for_team_id = ga.home_team_id THEN 'home'
        else 'away'
    END AS game_location,

    --5. Opponent Team ID
    CASE 
        when g.goal_for_team_id = ga.home_team_id then ga.away_team_id
        else ga.home_team_id
    end as goal_against_team_id

from {{ref('stg_goals')}} g 
join {{ref('fct_games')}} ga on g.game_id = ga.game_id