with goals_by_player as (
    select
        player_id,
        season_id,
        count(*) as goals_scored
    from {{ ref('stg_goals') }}
    group by player_id, season_id
)
select 
    player_id,
    season_id,
    goals_scored
from goals_by_player