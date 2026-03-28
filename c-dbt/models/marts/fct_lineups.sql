select 
    li.lineup_id,
    li.game_id,
    li.player_id,
    li.season_id,
    li.min_played,
    li.sub_in,
    li.sub_out,
    li.started,
    li.team_id,

    {{ game_location('li.team_id', 'ga.home_team_id') }} as game_location,

    {{ opponent_team_id('li.team_id', 'ga.home_team_id', 'ga.away_team_id') }} as opponent_team_id


from {{ref('stg_lineups')}} li
join {{ref('fct_games')}} ga on li.game_id = ga.game_id