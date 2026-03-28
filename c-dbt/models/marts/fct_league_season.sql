select
    league_id,
    season_id,
    count(distinct team_id)                                                            as team_count,
    count(*)                                                                           as total_team_games,
    sum(goals_scored)                                                                  as total_goals_scored,
    sum(goals_conceded)                                                                as total_goals_conceded,
    sum(points)                                                                        as total_points,
    sum(case when game_location = 'home' then goals_scored else 0 end)                as home_goals,
    sum(case when game_location = 'away' then goals_scored else 0 end)                as away_goals

from {{ ref('fct_team_games') }}
group by league_id, season_id
