-- Run once in Supabase SQL editor to create the DC tables.
-- Lives in analytics_analytics schema alongside the dbt models.

-- Team-level DC parameters (one row per team × season × fit_type)
-- fit_type: 'season' = full-season fit, 'form' = last-N-rounds fit
create table if not exists analytics_analytics.dim_team_dc_params (
    team_id    integer      not null,
    season_id  text,
    fit_type   text         not null default 'season',  -- 'season' | 'form'
    dc_attack  numeric(8,4) not null,  -- alpha normalised: 1.0 = league avg attack
    dc_defense numeric(8,4) not null,  -- 1/beta normalised: 1.0 = league avg (higher = better)
    dc_gamma   numeric(8,4) not null,  -- home advantage multiplier (~1.1–1.3)
    dc_rho     numeric(8,4) not null,  -- low-score correction term
    fitted_at  timestamptz  not null default now(),
    primary key (team_id, season_id, fit_type)
);

-- Per-match DC outputs (xG and xPts for both sides)
-- home_xpts / away_xpts are SCHEDULE-ADJUSTED: beating a strong side gives more xPts
create table if not exists analytics_analytics.dim_game_dc_xpts (
    game_id      integer      not null,
    season_id    text,
    fit_type     text         not null default 'season',
    home_team_id integer      not null,
    away_team_id integer      not null,
    home_xg      numeric(8,4),   -- model-predicted expected goals, home side
    away_xg      numeric(8,4),   -- model-predicted expected goals, away side
    home_xpts    numeric(8,4),   -- DC expected points, home side
    away_xpts    numeric(8,4),   -- DC expected points, away side
    p_home_win   numeric(8,4),
    p_draw       numeric(8,4),
    p_away_win   numeric(8,4),
    fitted_at    timestamptz  not null default now(),
    primary key (game_id, fit_type)
);

-- Grant write access so the API key used by dc_fit.py can upsert
grant all on analytics_analytics.dim_team_dc_params to anon, authenticated, service_role;
grant all on analytics_analytics.dim_game_dc_xpts    to anon, authenticated, service_role;
