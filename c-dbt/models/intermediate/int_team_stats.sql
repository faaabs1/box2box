with base as (
    select
        game_id,
        season_id,
        game_date,
        game_round,
        home_team_id as team_id,
        'home' as game_location,
        league_id,
        home_goals as goals_scored,
        away_goals as goals_conceded,
        home_gd as goal_difference,
        home_points as points,
        away_points as points_allowed,
        away_team_id as opponent_team_id  -- ADD THIS
    from {{ ref('stg_games') }}
    
    union all
    
    select
        game_id,
        season_id,
        game_date,
        game_round,
        away_team_id as team_id,
        'away' as game_location,
        league_id,
        away_goals as goals_scored,
        home_goals as goals_conceded,
        away_gd as goal_difference,
        away_points as points,
        home_points as points_allowed,
        home_team_id as opponent_team_id  -- ADD THIS
    from {{ ref('stg_games') }}
)
select 
    b.*
from base b
