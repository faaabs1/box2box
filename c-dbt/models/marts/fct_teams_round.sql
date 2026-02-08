with round_stats as (
    -- Your existing aggregation logic
    select
        team_id,
        season_id,
        game_round,
        game_location,
        count(*) as games_played,
        sum(points) as points,
        sum(goal_difference) as gd,
        sum(goals_scored) as goals_scored,
        sum(goals_conceded) as goals_conceded,
        sum(points_allowed) as points_allowed
    from {{ref('int_team_stats')}}
    group by 1, 2, 3, 4
),

calculated_stats as (
    -- 1. Keep the Home/Away splits
    select * from round_stats
    
    union all
    
    -- 2. Create the 'total' rows by ignoring game_location
    select 
        team_id,
        season_id,
        game_round,
        'total' as game_location,
        sum(games_played) as games_played,
        sum(points) as points,
        sum(gd) as gd,
        sum(goals_scored),
        sum(goals_conceded),
        sum(points_allowed)
    from round_stats
    group by 1, 2, 3, 4
)

select
    team_id,
    season_id,
    game_round,
    game_location,
    -- Now cumulative values work for all three categories independently!
    sum(points) over (
        partition by team_id, season_id, game_location 
        order by game_round
    ) as cumulative_points,

    sum(games_played) over (
        partition by team_id, season_id, game_location
        order by game_round
    ) as total_games_played,

    sum(gd) over (
        partition by team_id, season_id, game_location
        order by game_round
    ) as total_gd,

    sum(goals_scored) over (
        partition by team_id, season_id, game_location
        order by game_round
    ) as total_goals_scored,

    sum(goals_conceded) over (
        partition by team_id, season_id, game_location
        order by game_round
    ) as total_goals_conceded,

    sum(points_allowed) over (
        partition by team_id, season_id, game_location
        order by game_round
    ) as total_points_allowed,




    -- 3. Calculate form metrics (last 5 games)
    sum(points) over (
        partition by team_id, season_id, game_location
        order by game_round
        rows between 4 preceding and current row
    ) as form_points_last_5


from calculated_stats