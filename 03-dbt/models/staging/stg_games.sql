select 
    *, 
    EXTRACT(ISODOW FROM game_date) AS day_of_week_num,
    TRIM(TO_CHAR(game_date, 'Day')) AS day_name,

    CASE 
        WHEN home_goals > away_goals THEN 3
        WHEN home_goals = away_goals THEN 1
        ELSE 0 
    END as home_points,

    CASE 
        WHEN home_goals < away_goals THEN 3
        WHEN home_goals = away_goals THEN 1
        ELSE 0 
    END as away_points,

    home_goals - away_goals as home_GD,
    away_goals - home_goals as away_GD

from {{source('box2box_raw', 'games')}}