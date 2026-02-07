select 
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.min_played,
    li.sub_in,
    li.sub_out,
    li.started,
    li.team_id,

    case
        when li.team_id = ga.home_team_id then 'home'
        else 'away'
    end as game_location,

    case 
        when li.team_id = ga.home_team_id then ga.away_team_id
        else ga.home_team_id
    end as opponent_team_id


from {{ref('stg_lineups')}} li
join {{ref('fct_games')}} ga on li.game_id = ga.game_id