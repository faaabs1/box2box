with player_goals as (
    select 
        player_id,
        season_id,
        game_location, -- Ensure your goals table has this, or join to games to get it
        count(*) as total_goals_scored
    from {{ ref('fct_goals') }}
    group by 1, 2, 3
),

player_lineups as (
    select
        player_id,
        season_id,
        game_location,
        count(game_id) as games_played,
        sum(min_played) as total_minutes_played,
        sum(case when started then 1 else 0 end) as games_started,
        sum(case when sub_in is not null then 1 else 0 end) as count_sub_ins,
        sum(case when sub_out is not null then 1 else 0 end) as count_sub_outs
    from {{ ref('fct_lineups') }}
    group by 1, 2, 3
)

select
    li.*,
    coalesce(pg.total_goals_scored, 0) as total_goals_scored
from player_lineups li
left join player_goals pg 
    on li.player_id = pg.player_id 
    and li.season_id = pg.season_id
    and li.game_location = pg.game_location