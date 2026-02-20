select
    ps.player_id,
    ps.season_id,
    ps.game_location,
    ps.goals_scored,
    pl.minutes_played,
    pl.games_started,
    pl.times_subbed_in,
    pl.times_subbed_out,
    case 
        when pl.games_started > 0 then round(pl.minutes_played::numeric / pl.games_started, 2)
        else 0 
    end as avg_minutes_per_start
from {{ ref('int_player_stats') }} ps
left join {{ ref('int_player_lineup') }} pl
    on ps.player_id = pl.player_id
    and ps.season_id = pl.season_id
    and ps.game_location = pl.game_location