"""
Dixon-Coles model fitter
========================
Reads match results from analytics_analytics.fct_team_games, fits attack (alpha)
and defense (beta) parameters per team plus home advantage (gamma) via maximum
likelihood (L-BFGS-B), then upserts results into:

  public.dim_team_dc_params  — normalised team ratings
  public.dim_game_dc_xpts    — per-match expected goals + schedule-adjusted xPts

Normalisation:
  dc_attack  = alpha_i / mean(alpha)    (1.0 = league-average attack)
  dc_defense = mean(beta) / beta_i      (1.0 = league-average defence, inverted)

Usage:
  conda run -n box2box python b_data_entry_program/dc_fit.py --season 1
  conda run -n box2box python b_data_entry_program/dc_fit.py --season 1 --form-rounds 5
  conda run -n box2box python b_data_entry_program/dc_fit.py --season 1 --dry-run
"""

import os
import sys
import argparse
import warnings
from itertools import product as iproduct

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import poisson
from dotenv import load_dotenv
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCHEMA    = "analytics_analytics"
FIT_RHO   = True
MAX_GOALS = 10   # Poisson truncation for xPts outer product


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_matches(client, season_id, last_n_rounds=None):
    """
    Pull home-perspective rows only (avoids double-counting).
    When last_n_rounds is set, keep only the most recent N distinct game_round values.
    """
    q = (
        client.schema(SCHEMA)
        .from_("fct_team_games")
        .select(
            "game_id,team_id,opponent_team_id,"
            "goals_scored,goals_conceded,"
            "game_location,game_round,game_date,season_id"
        )
        .eq("game_location", "home")
    )
    if season_id:
        q = q.eq("season_id", season_id)

    resp = q.execute()
    if not resp.data:
        raise RuntimeError("No home-side match rows returned. Check season_id or data sync.")

    df = pd.DataFrame(resp.data).rename(columns={
        "team_id":          "home_team",
        "opponent_team_id": "away_team",
        "goals_scored":     "home_goals",
        "goals_conceded":   "away_goals",
    })
    df["home_goals"] = df["home_goals"].astype(int)
    df["away_goals"] = df["away_goals"].astype(int)
    df["game_round"] = pd.to_numeric(df["game_round"], errors="coerce")

    if last_n_rounds is not None:
        recent = df["game_round"].dropna().drop_duplicates().nlargest(last_n_rounds)
        df = df[df["game_round"].isin(recent)]
        print(f"  Form mode: rounds {sorted(recent.tolist())} ({len(df)} matches)")

    sn = season_id or (df["season_id"].iloc[0] if len(df) else "?")
    print(f"  Loaded {len(df)} matches, season={sn}")
    return df


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
def _tau(x, y, lam, mu, rho):
    """Dixon-Coles low-score correction for joint Poisson independence."""
    if   x == 0 and y == 0: return 1.0 - lam * mu * rho
    elif x == 0 and y == 1: return 1.0 + lam * rho
    elif x == 1 and y == 0: return 1.0 + mu  * rho
    elif x == 1 and y == 1: return 1.0 - rho
    return 1.0


def _unpack(params, n, fit_rho):
    """Extract (alpha, beta, gamma, rho) from log-space params array."""
    alpha = np.exp(params[:n]);  alpha[0] = 1.0
    beta  = np.exp(params[n:2*n])
    gamma = float(np.exp(params[2*n]))
    rho   = float(params[2*n+1]) if fit_rho else 0.0
    return alpha, beta, gamma, rho


def neg_log_likelihood(params, matches, teams, fit_rho):
    n = len(teams)
    alpha, beta, gamma, rho = _unpack(params, n, fit_rho)
    team_idx = {t: i for i, t in enumerate(teams)}
    ll = 0.0

    for _, row in matches.iterrows():
        i = team_idx[row["home_team"]]
        j = team_idx[row["away_team"]]
        lam = alpha[i] * beta[j] * gamma
        mu  = alpha[j] * beta[i]
        x, y = int(row["home_goals"]), int(row["away_goals"])
        t = _tau(x, y, lam, mu, rho)
        if t <= 0:
            return 1e9
        ll += np.log(t) + poisson.logpmf(x, lam) + poisson.logpmf(y, mu)

    return -ll


def fit(matches, fit_rho=True):
    """
    Fit Dixon-Coles model. Returns dict with raw params + normalised ratings:
      alpha_norm (dc_attack) = alpha_i / mean(alpha)   -- 1.0 = league avg attack
      beta_norm (dc_defense) = mean(beta) / beta_i     -- 1.0 = league avg (inverted)
    """
    teams = sorted(set(matches["home_team"]) | set(matches["away_team"]))
    n = len(teams)
    print(f"  Fitting {n} teams over {len(matches)} matches (rho={'yes' if fit_rho else 'no'})")

    x0 = np.zeros(2 * n + 1 + (1 if fit_rho else 0))
    x0[2 * n] = np.log(1.15)
    bounds = (
        [(None, None)] * (2 * n + 1)
        + ([(-0.99, 0.99)] if fit_rho else [])
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = minimize(
            neg_log_likelihood, x0,
            args=(matches, teams, fit_rho),
            method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 2000, "ftol": 1e-10},
        )

    if not result.success:
        print(f"  WARNING: optimiser did not fully converge: {result.message}")

    alpha, beta, gamma, rho = _unpack(result.x, n, fit_rho)
    alpha_norm = alpha / alpha.mean()   # dc_attack:  1.0 = league avg
    beta_norm  = beta.mean() / beta     # dc_defense: 1.0 = league avg, inverted

    print(f"  gamma={gamma:.3f}  rho={rho:.4f}" if fit_rho else f"  gamma={gamma:.3f}")

    return {
        "teams":      teams,
        "alpha":      alpha,
        "beta":       beta,
        "alpha_norm": alpha_norm,   # -> dc_attack in DB
        "beta_norm":  beta_norm,    # -> dc_defense in DB
        "gamma":      gamma,
        "rho":        rho,
        "params":     result.x,
    }


# ---------------------------------------------------------------------------
# Per-match expected goals + schedule-adjusted xPts
# ---------------------------------------------------------------------------
def compute_match_xpts(fit_result, matches):
    """
    For each match compute:
      home_xg / away_xg      : model-predicted expected goals
      p_home_win/p_draw/p_away_win : Poisson outer product with tau correction
      home_xpts / away_xpts  : 3*P(win) + 1*P(draw)
    Returns a DataFrame with one row per game_id.
    """
    alpha    = fit_result["alpha"]
    beta     = fit_result["beta"]
    gamma    = fit_result["gamma"]
    rho      = fit_result["rho"]
    team_idx = {t: i for i, t in enumerate(fit_result["teams"])}
    goals    = np.arange(MAX_GOALS + 1)
    rows     = []

    for _, row in matches.iterrows():
        hi  = team_idx[row["home_team"]];  ai = team_idx[row["away_team"]]
        lam = float(alpha[hi] * beta[ai] * gamma)
        mu  = float(alpha[ai] * beta[hi])

        mat = np.outer(poisson.pmf(goals, lam), poisson.pmf(goals, mu))
        for x, y in iproduct(range(2), range(2)):
            mat[x, y] *= _tau(x, y, lam, mu, rho)
        mat /= mat.sum()   # normalise after tau shifts mass

        p_hw = float(np.tril(mat, -1).sum())
        p_d  = float(np.trace(mat))
        p_aw = float(np.triu(mat,  1).sum())

        rows.append({
            "game_id":      int(row["game_id"]),
            "season_id":    row["season_id"],
            "home_team_id": int(row["home_team"]),
            "away_team_id": int(row["away_team"]),
            "home_xg":   round(lam, 4),
            "away_xg":   round(mu,  4),
            "home_xpts": round(3.0 * p_hw + p_d, 4),
            "away_xpts": round(3.0 * p_aw + p_d, 4),
            "p_home_win": round(p_hw, 4),
            "p_draw":     round(p_d,  4),
            "p_away_win": round(p_aw, 4),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DB upserts
# ---------------------------------------------------------------------------
def upsert_team_params(client, fit_result, season_id, fit_type):
    rows = [
        {
            "team_id":    int(t),
            "season_id":  season_id,
            "fit_type":   fit_type,
            "dc_attack":  round(float(fit_result["alpha_norm"][i]), 4),
            "dc_defense": round(float(fit_result["beta_norm"][i]),  4),
            "dc_gamma":   round(float(fit_result["gamma"]),         4),
            "dc_rho":     round(float(fit_result["rho"]),           4),
        }
        for i, t in enumerate(fit_result["teams"])
    ]
    client.schema(SCHEMA) \
        .from_("dim_team_dc_params") \
        .upsert(rows, on_conflict="team_id,season_id,fit_type") \
        .execute()
    print(f"  Upserted {len(rows)} rows -> dim_team_dc_params (fit_type='{fit_type}')")


def upsert_game_xpts(client, xpts_df, fit_type):
    rows = xpts_df.assign(fit_type=fit_type).to_dict("records")
    client.schema(SCHEMA) \
        .from_("dim_game_dc_xpts") \
        .upsert(rows, on_conflict="game_id,fit_type") \
        .execute()
    print(f"  Upserted {len(rows)} rows -> dim_game_dc_xpts (fit_type='{fit_type}')")


def print_table(fit_result, team_names):
    rows = sorted(
        zip(fit_result["teams"], fit_result["alpha_norm"], fit_result["beta_norm"]),
        key=lambda r: -r[1],
    )
    print(f"\n{'Team':<28} {'ATT':>6} {'DEF':>6}")
    print("-" * 42)
    for team_id, atk, dfs in rows:
        name = team_names.get(team_id, str(team_id))
        print(f"{name:<28} {atk:>6.3f} {dfs:>6.3f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Fit Dixon-Coles model and upsert to Supabase")
    parser.add_argument("--season",      default=None, help="season_id (default: latest in DB)")
    parser.add_argument("--form-rounds", type=int, default=None, metavar="N",
                        help="fit only the last N rounds (form mode)")
    parser.add_argument("--dry-run",     action="store_true",
                        help="print results but skip DB writes")
    args = parser.parse_args()

    fit_type = f"form{args.form_rounds}" if args.form_rounds else "season"

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        sys.exit("SUPABASE_URL and SUPABASE_KEY must be set in .env")

    client = create_client(url, key)

    print(f"\n=== Dixon-Coles fit  [fit_type={fit_type}] ===")
    matches = load_matches(client, args.season, args.form_rounds)

    # Resolve season_id — PK column cannot be NULL
    season_id = args.season or str(matches["season_id"].iloc[0])
    print(f"  season_id={season_id}")

    if len(matches) < 20:
        print(f"  WARNING: only {len(matches)} matches — params will be noisy. Recommend 30+.")

    result = fit(matches, fit_rho=FIT_RHO)

    # Fetch team names for display
    nr = client.schema(SCHEMA).from_("dim_teams").select("team_id,team_name").execute()
    team_names = {r["team_id"]: r["team_name"] for r in (nr.data or [])}
    print_table(result, team_names)

    xpts_df = compute_match_xpts(result, matches)
    tot = (
        xpts_df.groupby("home_team_id")["home_xpts"].sum()
        .add(xpts_df.groupby("away_team_id")["away_xpts"].sum(), fill_value=0)
        .sort_values(ascending=False)
    )
    print(f"\n{'Team':<28} {'DC xPts':>8}")
    print("-" * 38)
    for tid, xp in tot.items():
        print(f"{team_names.get(tid, str(tid)):<28} {xp:>8.1f}")

    if not args.dry_run:
        upsert_team_params(client, result, season_id, fit_type)
        upsert_game_xpts(client, xpts_df, fit_type)
    else:
        print("\n[dry-run] DB writes skipped.")

    print("\nDone.")


if __name__ == "__main__":
    main()
