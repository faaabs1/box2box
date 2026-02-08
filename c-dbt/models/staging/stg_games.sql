select 
    g.*,
    s.season_id,
    EXTRACT(ISODOW FROM game_date) AS day_of_week_num,
    TRIM(TO_CHAR(game_date, 'Day')) AS day_name,

    CASE 
        WHEN g.home_goals > g.away_goals THEN 3
        WHEN g.home_goals = g.away_goals THEN 1
        ELSE 0 
    END as home_points,

    CASE 
        WHEN g.home_goals < g.away_goals THEN 3
        WHEN g.home_goals = g.away_goals THEN 1
        ELSE 0 
    END as away_points,

    g.home_goals - g.away_goals as home_GD,
    g.away_goals - g.home_goals as away_GD

from {{source('box2box_raw', 'games')}} g
join {{ ref('stg_seasons') }} s
    on g.game_date <= s.valid_to and g.game_date >= s.valid_from