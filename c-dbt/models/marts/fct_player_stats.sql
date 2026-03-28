with lineup_agg as (
    select
        player_id,
        season_id,
        sum(minutes_played)   as minutes_total,
        sum(games_started)    as starts_total,
        sum(times_subbed_in)  as subs_in_total,
        sum(games_started) + sum(times_subbed_in) as apps_total
    from {{ ref('int_player_lineup') }}
    group by player_id, season_id
),

-- Each goal weighted by the opponents DC defence rating.
-- Scoring vs dc_defense=2.0 → counts 2.0 adj goals; vs dc_defense=0.5 → counts 0.5.
-- Falls back to 1.0 per goal (raw count) when DC params are unavailable.
goals_adj as (
    select
        g.player_id,
        g.season_id,
        count(*)                                                 as goals_scored,
        round(sum(coalesce(dc.dc_defense, 1.0))::numeric, 2)   as adj_goals
    from {{ ref('fct_goals') }} g
    left join analytics_analytics.dim_team_dc_params dc
        on  g.goal_against_team_id = dc.team_id
        and g.season_id            = dc.season_id::integer
        and dc.fit_type            = 'season'
    where g.own_goal = false
    group by g.player_id, g.season_id
)

select
    p.player_id,
    p.team_id,
    la.season_id,
    la.minutes_total,
    la.starts_total,
    la.subs_in_total,
    la.apps_total,
    coalesce(g.goals_scored, 0)   as goals_total,
    coalesce(g.adj_goals,    0.0) as adj_goals

from {{ ref('stg_players') }} p
join lineup_agg la
    on p.player_id = la.player_id
left join goals_adj g
    on p.player_id = g.player_id and la.season_id = g.season_id