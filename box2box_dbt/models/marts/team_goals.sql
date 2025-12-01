with games as (
    -- Reference your raw data source directly
    select * 
    from {{ source('box2box_raw', 'games')}}
),

teams as (
    select * 
    from {{source('box2box_raw', 'teams')}}
),

-- 1. Calculate goals scored when playing AT HOME
home_stats as (
    select 
        home_team_id as team_id,
        count(game_id) as games_played_home,
        sum(home_goals) as goals_scored_home,
        sum(away_goals) as goals_conceded_home
    from games
    group by home_team_id
),

-- 2. Calculate goals scored when playing AWAY
away_stats as (
    select 
        away_team_id as team_id,
        count(game_id) as games_played_away,
        sum(away_goals) as goals_scored_away,
        sum(home_goals) as goals_conceded_away
    from games
    group by away_team_id
),

-- 3. Join everything together
combined as (
    select
        t.team_name,
        t.team_id,
        
        -- Home Stats
        coalesce(h.goals_scored_home, 0) as home_goals,
        h.games_played_home as home_games,
        
        -- Away Stats
        coalesce(a.goals_scored_away, 0) as away_goals,
        a.games_played_away as away_games,
        
        -- Total Stats
        (coalesce(h.goals_scored_home, 0) + coalesce(a.goals_scored_away, 0)) as total_goals,
        (coalesce(h.games_played_home, 0) + coalesce(a.games_played_away, 0)) as total_games
        
    from teams t
    left join home_stats h on t.team_id = h.team_id
    left join away_stats a on t.team_id = a.team_id
)

select * from combined
order by total_goals desc