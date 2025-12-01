with games as (
    select * from {{ source('box2box_raw', 'games') }}
),

lineups as (
    select * from {{ source('box2box_raw', 'lineups') }}
),

goals as (
    select * from {{ source('box2box_raw', 'goals') }}
),

players as (
    select * from {{ source('box2box_raw', 'players') }}
),

-- 1. Enrich Lineups with Game Location Info
-- We need to know if the team played Home or Away for each specific game
player_games as (
    select
        l.player_id,
        l.team_id,
        l.game_id,
        l.min_played,
        l.started,
        l.sub_in,
        l.sub_out,
        case
            when l.team_id = g.home_team_id then 'Home'
            when l.team_id = g.away_team_id then 'Away'
            else 'Unknown'
        end as location
    from lineups l
    join games g on l.game_id = g.game_id
),

-- 2. Aggregate Lineup Stats (Minutes, Starts, Subs)
lineup_stats as (
    select
        player_id,
        team_id,
        
        -- Home Stats
        sum(case when location = 'Home' then min_played else 0 end) as minutes_home,
        sum(case when location = 'Home' then "started"::int else 0 end) as starts_home,
        
        -- Away Stats
        sum(case when location = 'Away' then min_played else 0 end) as minutes_away,
        sum(case when location = 'Away' then "started"::int else 0 end) as starts_away,
        
        -- Total Stats
        sum(min_played) as minutes_total,
        sum("started"::int) as starts_total,
        sum(sub_in::int) as total_sub_ins,
        sum(sub_out::int) as total_sub_outs
    from player_games
    group by player_id, team_id
),

-- 3. Aggregate Goals
-- We calculate goals separately to avoid "fan-out" errors if we joined directly to lineups
goal_stats as (
    select
        gl.player_id,
        gl.goal_for as team_id,
        
        -- Check location by joining back to the game info
        sum(case when gl.goal_for = g.home_team_id then 1 else 0 end) as goals_home,
        sum(case when gl.goal_for = g.away_team_id then 1 else 0 end) as goals_away,
        count(*) as goals_total
    from goals gl
    join games g on gl.game_id = g.game_id
    where gl.player_id is not null -- Exclude Opponent Own Goals (where player_id might be null)
    group by gl.player_id, gl.goal_for
),

-- 4. Final Join
final as (
    select
        p.firstname || ' ' || p.lastname as player_name,
        p.player_id,
        ls.team_id,
        
        -- Home Stats
        coalesce(ls.minutes_home, 0) as minutes_home,
        coalesce(gs.goals_home, 0) as goals_home,
        coalesce(ls.starts_home, 0) as starts_home,
        
        -- Away Stats
        coalesce(ls.minutes_away, 0) as minutes_away,
        coalesce(gs.goals_away, 0) as goals_away,
        coalesce(ls.starts_away, 0) as starts_away,
        
        -- Total Stats
        coalesce(ls.minutes_total, 0) as minutes_total,
        coalesce(gs.goals_total, 0) as goals_total,
        coalesce(ls.starts_total, 0) as starts_total,
        coalesce(ls.total_sub_ins, 0) as sub_ins_total,
        coalesce(ls.total_sub_outs, 0) as sub_outs_total

    from lineup_stats ls
    join players p on ls.player_id = p.player_id
    left join goal_stats gs on ls.player_id = gs.player_id and ls.team_id = gs.team_id
)

select * from final
order by minutes_total desc