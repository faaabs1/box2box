select 
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.min_played,
    li.sub_in,
    li.sub_out,
    li.started,
    li.team_id,
    g.season_id
from {{source('box2box_raw', 'lineups')}} li
join {{ ref('stg_games') }} g
    on li.game_id = g.game_id