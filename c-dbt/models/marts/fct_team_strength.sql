-- Team strength ratings, normalised against league average per season.
--
-- When Dixon-Coles params exist (run dc_fit.py first):
--   attack_rating  : dc_attack  — opponent-adjusted, jointly estimated
--   defence_rating : dc_defense — opponent-adjusted, jointly estimated
--   dc_xpts        : schedule-adjusted expected points (P(win/draw) × 3/1 summed per match)
--
-- Fallback when DC params are absent (early season / before first fit):
--   attack_rating  : team GF/game ÷ league avg GF/game  (1.0 = average)
--   defence_rating : league avg GA/game ÷ team GA/game  (1.0 = average, inverted)
--   dc_xpts        : NULL (pyth_points available as alternative)
--
-- form_rating    : team pts/game (last 5) ÷ league avg pts/game  (always rolling, no DC)
-- overall_strength: 40% attack + 40% defence + 20% form
-- pyth_points    : Pythagorean expectation (Anderson & Dowie, exponent 1.8) — kept as reference
-- pyth_rank      : league rank by pyth_points

with team_season as (
    select
        team_id,
        league_id,
        season_id,
        sum(games_played)   as games_played,
        sum(goals_scored)   as goals_scored,
        sum(goals_conceded) as goals_conceded,
        sum(points)         as points
    from {{ ref('fct_teams_season') }}
    group by team_id, league_id, season_id
),

league_avgs as (
    select
        league_id,
        season_id,
        avg(goals_scored::float   / nullif(games_played, 0)) as avg_gf_per_game,
        avg(goals_conceded::float / nullif(games_played, 0)) as avg_ga_per_game,
        avg(points::float         / nullif(games_played, 0)) as avg_pts_per_game
    from team_season
    group by league_id, season_id
),

latest_form as (
    -- most recent cumulative form window per team per season
    select distinct on (team_id, season_id)
        team_id,
        season_id,
        form_points_last_5
    from {{ ref('fct_teams_round') }}
    where game_location = 'total'
    order by team_id, season_id, game_round desc
),

-- Dixon-Coles params (NULL when dc_fit.py has not been run yet)
dc_params as (
    select
        team_id,
        season_id::integer,
        dc_attack,
        dc_defense
    from analytics_analytics.dim_team_dc_params
    where fit_type = 'season'
),

-- Opponent-adjusted goals scored: each goal weighted by the DC defence rating of the
-- opponent. Scoring vs dc_defense=2.0 → 2.0 adj goals; vs 0.5 → 0.5. Falls back to
-- 1.0 per goal (= raw count) when DC params are not available for that opponent.
team_adj_goals as (
    select
        g.goal_for_team_id               as team_id,
        g.season_id,
        round(sum(coalesce(dc.dc_defense, 1.0))::numeric, 1) as adj_goals_for
    from {{ ref('fct_goals') }} g
    left join analytics_analytics.dim_team_dc_params dc
        on  g.goal_against_team_id = dc.team_id
        and g.season_id            = dc.season_id::integer
        and dc.fit_type            = 'season'
    where g.own_goal = false
    group by g.goal_for_team_id, g.season_id
),

-- DC model expected goals per team per season (home + away combined)
team_xg_agg as (
    select season_id::integer as season_id, home_team_id as team_id, sum(home_xg) as xg_for
    from analytics_analytics.dim_game_dc_xpts
    where fit_type = 'season'
    group by season_id::integer, home_team_id

    union all

    select season_id::integer, away_team_id, sum(away_xg)
    from analytics_analytics.dim_game_dc_xpts
    where fit_type = 'season'
    group by season_id::integer, away_team_id
),

team_xg as (
    select team_id, season_id, round(sum(xg_for)::numeric, 1) as xg_for
    from team_xg_agg
    group by team_id, season_id
),

-- Schedule-adjusted xPts: sum DC expected points per team per season
dc_xpts_agg as (
    select season_id::integer, home_team_id as team_id, sum(home_xpts) as dc_xpts
    from analytics_analytics.dim_game_dc_xpts
    where fit_type = 'season'
    group by season_id::integer, home_team_id

    union all

    select season_id::integer, away_team_id as team_id, sum(away_xpts) as dc_xpts
    from analytics_analytics.dim_game_dc_xpts
    where fit_type = 'season'
    group by season_id::integer, away_team_id
),

dc_xpts as (
    select team_id, season_id, sum(dc_xpts) as dc_xpts
    from dc_xpts_agg
    group by team_id, season_id
),

base as (
    select
        ts.team_id,
        ts.league_id,
        ts.season_id,
        ts.games_played,

        -- per-game rates (kept for reference)
        round(ts.goals_scored::numeric   / nullif(ts.games_played, 0), 2) as gf_per_game,
        round(ts.goals_conceded::numeric / nullif(ts.games_played, 0), 2) as ga_per_game,
        round(ts.points::numeric         / nullif(ts.games_played, 0), 2) as pts_per_game,

        -- Attack: DC when available, else naive GF/game ratio
        round(coalesce(
            dc.dc_attack,
            (ts.goals_scored::numeric / nullif(ts.games_played, 0))
            / nullif(la.avg_gf_per_game::numeric, 0)
        ), 2) as attack_rating,

        -- Defence: DC when available, else naive GA ratio (inverted)
        round(coalesce(
            dc.dc_defense,
            la.avg_ga_per_game::numeric
            / nullif(ts.goals_conceded::numeric / nullif(ts.games_played, 0), 0)
        ), 2) as defence_rating,

        coalesce(lf.form_points_last_5, 0) as form_pts_last_5,

        round(
            (coalesce(lf.form_points_last_5, 0)::numeric / 5)
            / nullif(la.avg_pts_per_game::numeric, 0)
        , 2) as form_rating,

        -- Overall composite (attack/defence source is DC or naive, form always rolling)
        round(
            (
                  0.40 * coalesce(
                      dc.dc_attack,
                      (ts.goals_scored::numeric / nullif(ts.games_played, 0)) / nullif(la.avg_gf_per_game::numeric, 0)
                  )
                + 0.40 * coalesce(
                      dc.dc_defense,
                      la.avg_ga_per_game::numeric / nullif(ts.goals_conceded::numeric / nullif(ts.games_played, 0), 0)
                  )
                + 0.20 * ((coalesce(lf.form_points_last_5, 0)::numeric / 5) / nullif(la.avg_pts_per_game::numeric, 0))
            )
        , 2) as overall_strength,

        -- Schedule-adjusted xPts from DC (NULL until dc_fit.py has been run)
        round(dx.dc_xpts::numeric, 1) as dc_xpts,

        -- Opponent-adjusted goals for (NULL until dc_fit.py has been run)
        tag.adj_goals_for,

        -- DC model expected goals for (NULL until dc_fit.py has been run)
        txg.xg_for,

        -- Actual goals minus model-predicted xG (positive = overperforming model)
        case when ts.goals_scored is not null and txg.xg_for is not null
            then round((ts.goals_scored - txg.xg_for)::numeric, 1)
        end as goals_above_xg,

        -- Pythagorean xPts kept as reference / fallback
        round(
            power(ts.goals_scored::numeric, 1.8)
            / nullif(
                power(ts.goals_scored::numeric, 1.8) + power(ts.goals_conceded::numeric, 1.8),
                0
            )
            * ts.games_played * 3
        , 1) as pyth_points

    from team_season ts
    join league_avgs la
        on ts.league_id = la.league_id
        and ts.season_id = la.season_id
    left join latest_form lf
        on ts.team_id   = lf.team_id
        and ts.season_id = lf.season_id
    left join dc_params dc
        on ts.team_id   = dc.team_id
        and ts.season_id = dc.season_id
    left join dc_xpts dx
        on ts.team_id   = dx.team_id
        and ts.season_id = dx.season_id
    left join team_adj_goals tag
        on ts.team_id   = tag.team_id
        and ts.season_id = tag.season_id
    left join team_xg txg
        on ts.team_id   = txg.team_id
        and ts.season_id = txg.season_id
)

select
    *,
    rank() over (
        partition by league_id, season_id
        order by pyth_points desc nulls last
    )::int as pyth_rank,
    rank() over (
        partition by league_id, season_id
        order by dc_xpts desc nulls last
    )::int as dc_xpts_rank
from base
