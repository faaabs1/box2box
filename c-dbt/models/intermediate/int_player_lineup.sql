with base as (
    select
        li.player_id,
        li.season_id,
        case
            when li.team_id = g.home_team_id then 'home'
            else 'away'
        end as game_location,
        li.min_played,
        case when li.started = TRUE then 1 else 0 end as started,
        case when li.sub_in = TRUE then 1 else 0 end as sub_in,
        case when li.sub_out = TRUE then 1 else 0 end as sub_out
    from {{ ref('stg_lineups') }} li
    join {{ ref('fct_games') }} g on li.game_id = g.game_id
)
select 
    player_id,
    season_id,
    game_location,
    sum(min_played) as minutes_played,
    sum(started) as games_started,
    sum(sub_in) as times_subbed_in,
    sum(sub_out) as times_subbed_out
from base
group by player_id, season_id, game_location