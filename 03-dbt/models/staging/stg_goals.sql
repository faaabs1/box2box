select 
    g.goal_id,
    g.game_id,
    g.goal_min,
    g.player_id,
    g.game_situation,
    g.own_goal,
    g.goal_for as goal_for_team_id,

    -- 2. Time Buckets (15-min intervals)
    CASE 
        WHEN goal_min BETWEEN 0 AND 15 THEN '0-15'
        WHEN goal_min BETWEEN 16 AND 30 THEN '16-30'
        WHEN goal_min BETWEEN 31 AND 45 THEN '31-45'
        WHEN goal_min BETWEEN 46 AND 60 THEN '46-60'
        WHEN goal_min BETWEEN 61 AND 75 THEN '61-75'
        ELSE '76-90+'
    END AS goal_time_bucket,

    -- 3. Half of the Match
    CASE 
        WHEN goal_min <= 45 THEN 'First Half'
        ELSE 'Second Half'
    END AS goal_half,

    --4. Goal Location (Home/Away)
    CASE 
        when g.goal_for = ga.home_team_id THEN 'home'
        else 'away'
    END AS goal_location,

    --5. Opponent Team ID
    CASE 
        when g.goal_for = ga.home_team_id then ga.away_team_id
        else ga.home_team_id
    end as goal_against_team_id

from {{source('box2box_raw', 'goals')}} g 
join {{source('box2box_raw', 'games')}} ga on g.game_id = ga.game_id