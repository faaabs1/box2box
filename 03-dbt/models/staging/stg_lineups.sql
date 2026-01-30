select 
    lineup_id,
    game_id,
    player_id,
    min_played,
    sub_in,
    sub_out,
    started,
    team_id,

    case
        when li.team_id = ga.home_team_id then 'home'
        else 'away'
    end as game_location,

    case 
        when li.team_id = ga.home_team_id then ga.away_team_id
        else ga.home_team_id
    end as opponent_team_id,

    CASE 
        when sub_out = TRUE and started = TRUE then 90-min_played
        else NULL
        end as sub_out_min

from {{source('box2box_raw', 'lineups')}} li,
join {{source('box2box_raw', 'games')}} ga on li.game_id = ga.game_id