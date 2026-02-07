with home_team_stats as (
    select
        home_team_id as team_id,
        'home' as game_location,
        sum(home_goals) as goals_scored,
        sum(away_goals) as goals_conceded,
        sum(home_gd) as goal_diff,
        sum(home_points) as points,
        sum(away_points) as points_allowed
    from {{ref('fct_games')}}
    group by home_team_id
), 

away_team_stats as (
    select
        away_team_id as team_id,
        'away' as game_location,
        sum(away_goals) as goals_scored,
        sum(home_goals) as goals_conceded,
        sum(away_gd) as goal_diff,
        sum(away_points) as points,
        sum(home_points) as points_allowed
    from {{ref('fct_games')}}
    group by away_team_id
) 

select * from home_team_stats
union all
select * from away_team_stats