with base as (
    select
        player_id,
        season_id,
        'home' as game_location,
        min_played,
        case when started = TRUE then 1 else 0 end as started,
        case when sub_in = TRUE then 1 else 0 end as sub_in,
        case when sub_out = TRUE then 1 else 0 end as sub_out
    from {{ ref('stg_lineups') }}
    
    union all
    
    select
        player_id,
        season_id,
        'away' as game_location,
        min_played as min_played,
        case when started = TRUE then 1 else 0 end as started,
        case when sub_in = TRUE then 1 else 0 end as sub_in,
        case when sub_out = TRUE then 1 else 0 end as sub_out
    from {{ ref('stg_lineups') }}
)
select 
    player_id,
    season_id,
    game_location,
    sum(min_played) as min_played,
    sum(started) as games_started,
    sum(sub_in) as sub_in,
    sum(sub_out) as sub_out
from base
group by player_id, game_location, season_id