with base as (
    select
        player_id,
        season_id,
        'home' as game_location,
        1 as goals_scored
    from {{ ref('stg_goals') }}
    
    union all
    
    select
        player_id,
        season_id,
        'away' as game_location,
        1 as goals_scored
    from {{ ref('stg_goals') }}
)
select 
    player_id,
    season_id,
    game_location,
    sum(goals_scored) as goals_scored
from base
group by player_id, game_location, season_id