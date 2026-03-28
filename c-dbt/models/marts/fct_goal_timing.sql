select
    g.goal_id,
    g.game_id,
    g.goal_min,
    g.player_id,
    g.goal_for_team_id,
    g.goal_against_team_id,
    g.game_location,
    g.game_situation,
    g.own_goal,

    ga.game_date,
    ga.season_id,
    ga.home_team_league_id as league_id,

    case
        when g.goal_min <= 15 then '0-15'
        when g.goal_min <= 30 then '16-30'
        when g.goal_min <= 45 then '31-45'
        when g.goal_min <= 60 then '46-60'
        when g.goal_min <= 75 then '61-75'
        else '76-90+'
    end as goal_timing_bucket,

    case
        when g.goal_min <= 15 then 1
        when g.goal_min <= 30 then 2
        when g.goal_min <= 45 then 3
        when g.goal_min <= 60 then 4
        when g.goal_min <= 75 then 5
        else 6
    end as bucket_order

from {{ ref('fct_goals') }} g
join {{ ref('fct_games') }} ga
    on g.game_id = ga.game_id
