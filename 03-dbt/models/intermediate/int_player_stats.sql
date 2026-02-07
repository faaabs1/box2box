with base as (
    select
        player_id,
        'home' as game_location,
        count(goal_id) as goals_scored
    from {{ ref('stg_goals') }}
    
    union all
    
    select
        player_id,
        'away' as game_location,
        count(goal_id) as goals_scored
    from {{ ref('stg_goals') }}
)
select 
    b.*,
    s.season_id 
from base b
join {{ ref('stg_seasons')}} s 
    on b.game_date <= s.valid_to and b.game_date >= s.valid_from