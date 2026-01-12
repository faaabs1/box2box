with games as (
    select * from {{ source('box2box_raw', 'games')}}
),

teams as (
    select * from {{source('box2box_raw', 'teams')}}
),

home_stats as (
    select 
        home_team_id as team_id,
        count(game_id) as games_played_home,
        sum(home_goals) as goals_scored_home,
        sum(away_goals) as goals_conceded_home,
        sum(CASE 
            WHEN home_goals > away_goals THEN 3
            WHEN home_goals = away_goals THEN 1
            ELSE 0 
        END) as home_points
    from games
    group by home_team_id
),

away_stats as (
    select 
        away_team_id as team_id,
        count(game_id) as games_played_away,
        sum(away_goals) as goals_scored_away,
        sum(home_goals) as goals_conceded_away,
        sum(CASE 
            WHEN home_goals < away_goals THEN 3
            WHEN home_goals = away_goals THEN 1
            ELSE 0 
        END) as away_points
    from games
    group by away_team_id
),

combined as (
    select
        t.team_name,
        t.team_id,
        
        -- Home Stats
        coalesce(h.goals_scored_home, 0) as home_goals,
        coalesce(h.home_points, 0) as home_points,
        coalesce(h.goals_conceded_home, 0) as home_goals_conceded,
        coalesce(h.games_played_home, 0) as home_games, -- Added coalesce here for safety

        -- FIX: Cast to FLOAT before dividing
        (CAST(coalesce(h.goals_scored_home, 0) AS FLOAT) / nullif(h.games_played_home, 0)) as avg_goals_home,
        (CAST(coalesce(h.goals_conceded_home, 0) AS FLOAT) / nullif(h.games_played_home, 0)) as avg_goals_conceded_home,

        -- Away Stats
        coalesce(a.goals_scored_away, 0) as away_goals,
        coalesce(a.away_points, 0) as away_points,
        coalesce(a.goals_conceded_away, 0) as away_goals_conceded,
        coalesce(a.games_played_away, 0) as away_games, -- Added coalesce here for safety

        -- FIX: Cast to FLOAT before dividing
        (CAST(coalesce(a.goals_scored_away, 0) AS FLOAT) / nullif(a.games_played_away, 0)) as avg_goals_away,
        (CAST(coalesce(a.goals_conceded_away, 0) AS FLOAT) / nullif(a.games_played_away, 0)) as avg_goals_conceded_away,
        
        -- Total Stats
        (coalesce(h.goals_scored_home, 0) + coalesce(a.goals_scored_away, 0)) as total_goals,
        (coalesce(h.home_points, 0) + coalesce(a.away_points, 0)) as total_points,
        (coalesce(h.goals_conceded_home, 0) + coalesce(a.goals_conceded_away, 0)) as total_goals_conceded,
        (coalesce(h.games_played_home, 0) + coalesce(a.games_played_away, 0)) as total_games,

        -- FIX: Cast to FLOAT before dividing
        (CAST((coalesce(h.goals_conceded_home, 0) + coalesce(a.goals_conceded_away, 0)) AS FLOAT) 
            / nullif((coalesce(h.games_played_home, 0) + coalesce(a.games_played_away, 0)), 0)
        ) as avg_goals_conceded,

        (CAST((coalesce(h.goals_scored_home, 0) + coalesce(a.goals_scored_away, 0)) AS FLOAT) 
            / nullif((coalesce(h.games_played_home, 0) + coalesce(a.games_played_away, 0)), 0)
        ) as avg_goals_game
    
    from teams t
    left join home_stats h on t.team_id = h.team_id
    left join away_stats a on t.team_id = a.team_id
)

select * from combined
order by total_goals desc