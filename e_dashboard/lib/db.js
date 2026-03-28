const { createClient } = require('@supabase/supabase-js');

let client = null;

function getClient() {
  if (!client) {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_KEY;
    if (!url || !key) throw new Error('SUPABASE_URL and SUPABASE_KEY must be set in .env');
    client = createClient(url, key);
  }
  return client;
}

const SCHEMA = 'analytics_analytics';
const MY_TEAM_ID = parseInt(process.env.MY_TEAM_ID || '1', 10);

async function fetchLeagues() {
  const sb = getClient();
  const { data, error } = await sb.schema(SCHEMA).from('dim_leagues').select('league_id, league_name').order('league_name');
  if (error) throw error;
  return data;
}

async function fetchSeasons() {
  const sb = getClient();
  const { data, error } = await sb.schema(SCHEMA).from('dim_seasons').select('season_id, season_name').order('season_name', { ascending: false });
  if (error) throw error;
  return data;
}

async function fetchTeams(leagueId) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('dim_teams').select('team_id, team_name, team_abb').order('team_name');
  if (leagueId) query = query.eq('league_id', leagueId);
  const { data, error } = await query;
  if (error) throw error;
  return data;
}

async function fetchLeagueTable(leagueId, seasonId, location = null) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_teams_season')
    .select('team_id, league_id, season_id, game_location, goals_scored, goals_conceded, goal_difference, points, points_allowed, games_played');
  if (leagueId)  query = query.eq('league_id', leagueId);
  if (seasonId)  query = query.eq('season_id', seasonId);
  if (location)  query = query.eq('game_location', location);
  const { data, error } = await query;
  if (error) throw error;

  // Aggregate home + away per team
  const byTeam = {};
  for (const row of data) {
    if (!byTeam[row.team_id]) {
      byTeam[row.team_id] = { team_id: row.team_id, league_id: row.league_id, season_id: row.season_id, goals_scored: 0, goals_conceded: 0, goal_difference: 0, points: 0, games_played: 0 };
    }
    byTeam[row.team_id].goals_scored    += row.goals_scored    || 0;
    byTeam[row.team_id].goals_conceded  += row.goals_conceded  || 0;
    byTeam[row.team_id].goal_difference += row.goal_difference || 0;
    byTeam[row.team_id].points          += row.points          || 0;
    byTeam[row.team_id].games_played    += row.games_played    || 0;
  }

  // Attach team names
  const teams = await fetchTeams(leagueId);
  const teamMap = Object.fromEntries(teams.map(t => [t.team_id, t]));

  return Object.values(byTeam)
    .map(t => ({ ...t, team_name: teamMap[t.team_id]?.team_name || `Team ${t.team_id}`, team_abb: teamMap[t.team_id]?.team_abb }))
    .sort((a, b) => b.points - a.points || b.goal_difference - a.goal_difference || b.goals_scored - a.goals_scored);
}

async function fetchTopScorers(leagueId, seasonId, limit = 10, teamId = null) {
  const sb = getClient();
  let teamIds, teamMap;
  if (teamId) {
    const allTeams = await fetchTeams(null);
    teamMap = Object.fromEntries(allTeams.map(t => [t.team_id, t]));
    teamIds = [teamId];
  } else {
    const teams = await fetchTeams(leagueId);
    if (!teams.length) return [];
    teamIds = teams.map(t => t.team_id);
    teamMap = Object.fromEntries(teams.map(t => [t.team_id, t]));
  }

  let statsQ = sb.schema(SCHEMA).from('fct_player_stats')
    .select('player_id, team_id, goals_total, apps_total, minutes_total')
    .in('team_id', teamIds)
    .gt('goals_total', 0)
    .order('goals_total', { ascending: false })
    .limit(limit);
  if (seasonId) statsQ = statsQ.eq('season_id', seasonId);
  const { data: stats, error: sErr } = await statsQ;
  if (sErr) throw sErr;
  if (!stats || !stats.length) return [];

  const playerIds = stats.map(s => s.player_id);
  const { data: players, error: pErr } = await sb.schema(SCHEMA).from('dim_players')
    .select('player_id, firstname, lastname')
    .in('player_id', playerIds);
  if (pErr) throw pErr;
  const playerMap = Object.fromEntries((players || []).map(p => [p.player_id, `${p.firstname || ''} ${p.lastname || ''}`.trim()]));

  return stats.map(s => ({
    player_name: playerMap[s.player_id] || `#${s.player_id}`,
    team_name: teamMap[s.team_id]?.team_name || `Team ${s.team_id}`,
    goals: s.goals_total,
    apps: s.apps_total,
    minutes: s.minutes_total,
    g90: s.minutes_total >= 1 ? (s.goals_total / s.minutes_total * 90).toFixed(2) : '—',
  }));
}

async function fetchTeamGames(teamId, seasonId, leagueId, location) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_team_games')
    .select('game_id, game_date, game_round, game_location, goals_scored, goals_conceded, goal_difference, points, result, opponent_team_id, opponent_name')
    .eq('team_id', teamId);
  if (seasonId) query = query.eq('season_id', seasonId);
  if (leagueId) query = query.eq('league_id', leagueId);
  if (location) query = query.eq('game_location', location);
  const { data, error } = await query;
  if (error) throw error;
  return (data || []).sort((a, b) => (b.game_date || '').localeCompare(a.game_date || ''));
}

async function fetchTeamSeasonStats(teamId, seasonId, leagueId, location) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_teams_season')
    .select('*')
    .eq('team_id', teamId);
  if (seasonId) query = query.eq('season_id', seasonId);
  if (leagueId) query = query.eq('league_id', leagueId);
  if (location) query = query.eq('game_location', location);
  const { data, error } = await query;
  if (error) throw error;

  const agg = loc => {
    const rows = loc ? (data || []).filter(r => r.game_location === loc) : (data || []);
    return {
      goals_scored:    rows.reduce((s, r) => s + (r.goals_scored    || 0), 0),
      goals_conceded:  rows.reduce((s, r) => s + (r.goals_conceded  || 0), 0),
      goal_difference: rows.reduce((s, r) => s + (r.goal_difference || 0), 0),
      points:          rows.reduce((s, r) => s + (r.points          || 0), 0),
      games_played:    rows.reduce((s, r) => s + (r.games_played    || 0), 0),
    };
  };

  return {
    ...agg(null),
    home: agg('home'),
    away: agg('away'),
  };
}

async function fetchPlayers(teamId, seasonId) {
  const sb = getClient();

  let statsQuery = sb.schema(SCHEMA).from('fct_player_stats')
    .select('player_id, team_id, season_id, minutes_total, starts_total, subs_in_total, apps_total, goals_total');
  if (teamId) statsQuery = statsQuery.eq('team_id', parseInt(teamId, 10));
  if (seasonId) statsQuery = statsQuery.eq('season_id', seasonId);

  let playersQuery = sb.schema(SCHEMA).from('dim_players')
    .select('player_id, firstname, lastname, position1, position2, strong_foot, jersey_number, dob, team_id')
    .order('jersey_number');
  if (teamId) playersQuery = playersQuery.eq('team_id', parseInt(teamId, 10));

  const [statsRes, playersRes] = await Promise.all([statsQuery, playersQuery]);
  if (statsRes.error) throw statsRes.error;
  if (playersRes.error) throw playersRes.error;

  const statsMap = Object.fromEntries((statsRes.data || []).map(s => [s.player_id, s]));

  return (playersRes.data || []).map(p => {
    const s = statsMap[p.player_id] || {};
    const age = p.dob ? Math.floor((Date.now() - new Date(p.dob)) / (365.25 * 24 * 3600 * 1000)) : null;
    return {
      player_id: p.player_id,
      full_name: `${p.firstname || ''} ${p.lastname || ''}`.trim(),
      jersey_number: p.jersey_number,
      position1: p.position1,
      position2: p.position2,
      strong_foot: p.strong_foot,
      age,
      goals: s.goals_total || 0,
      minutes: s.minutes_total || 0,
      starts: s.starts_total || 0,
      apps: s.apps_total || 0,
      subs_in: s.subs_in_total || 0,
    };
  });
}

async function fetchPlayerGames(playerId) {
  const sb = getClient();
  const { data, error } = await sb.schema(SCHEMA).from('fct_player_games')
    .select('game_id, game_date, game_round, game_location, started, min_played, sub_in, sub_out, goals_scored, goals_conceded, goals_by_player, result, opponent_team_id, opponent_name')
    .eq('player_id', playerId)
    .order('game_date', { ascending: false });
  if (error) throw error;
  return (data || []).map(g => ({
    game_id: g.game_id,
    game_date: g.game_date,
    game_round: g.game_round,
    game_location: g.game_location,
    started: g.started,
    min_played: g.min_played,
    sub_in_min: g.sub_in,
    sub_out_min: g.sub_out,
    goals: g.goals_by_player || 0,
    goals_scored: g.goals_scored,
    goals_conceded: g.goals_conceded,
    result: g.result,
    opponent_name: g.opponent_name || `Team ${g.opponent_team_id}`,
  }));
}

async function fetchGameDetail(gameId) {
  const sb = getClient();
  const [gameRes, lineupsRes, goalsRes, allTeams] = await Promise.all([
    sb.schema(SCHEMA).from('fct_games')
      .select('game_id, game_date, game_round, home_team_id, away_team_id, home_goals, away_goals, season_id')
      .eq('game_id', gameId),
    sb.schema(SCHEMA).from('fct_lineups')
      .select('player_id, team_id, started, min_played, sub_in, sub_out')
      .eq('game_id', gameId),
    sb.schema(SCHEMA).from('fct_goals')
      .select('goal_id, goal_min, player_id, goal_for_team_id, own_goal, game_situation')
      .eq('game_id', gameId).order('goal_min'),
    fetchTeams(null),
  ]);
  if (gameRes.error) throw gameRes.error;
  if (!gameRes.data.length) throw new Error(`Game ${gameId} not found`);

  const game = gameRes.data[0];
  const teamMap = Object.fromEntries(allTeams.map(t => [t.team_id, t]));

  const allPlayerIds = [...new Set([
    ...(lineupsRes.data || []).map(l => l.player_id),
    ...(goalsRes.data || []).map(g => g.player_id),
  ])];
  let playerMap = {};
  if (allPlayerIds.length) {
    const { data: players } = await sb.schema(SCHEMA).from('dim_players')
      .select('player_id, firstname, lastname').in('player_id', allPlayerIds);
    for (const p of players || []) playerMap[p.player_id] = `${p.firstname || ''} ${p.lastname || ''}`.trim();
  }

  const lineups = (lineupsRes.data || []).map(l => ({
    ...l, name: playerMap[l.player_id] || `#${l.player_id}`,
  }));
  const goals = (goalsRes.data || []).map(g => ({
    ...g, player_name: playerMap[g.player_id] || `#${g.player_id}`,
  }));

  return {
    game,
    home_team: teamMap[game.home_team_id] || { team_name: `Team ${game.home_team_id}`, team_id: game.home_team_id },
    away_team: teamMap[game.away_team_id] || { team_name: `Team ${game.away_team_id}`, team_id: game.away_team_id },
    home_lineup: lineups.filter(l => l.team_id == game.home_team_id).sort((a, b) => b.started - a.started),
    away_lineup: lineups.filter(l => l.team_id == game.away_team_id).sort((a, b) => b.started - a.started),
    goals,
  };
}

async function fetchTeamGoalsTiming(teamId, seasonId, location) {
  const sb = getClient();
  let gamesQ = sb.schema(SCHEMA).from('fct_team_games')
    .select('game_id').eq('team_id', teamId);
  if (seasonId) gamesQ = gamesQ.eq('season_id', seasonId);
  if (location) gamesQ = gamesQ.eq('game_location', location);
  const { data: tgames, error: tgErr } = await gamesQ;
  if (tgErr) throw tgErr;

  const gameIds = (tgames || []).map(g => g.game_id);
  if (!gameIds.length) return [];

  const { data: goals, error } = await sb.schema(SCHEMA).from('fct_goals')
    .select('goal_time_bucket, goal_for_team_id').in('game_id', gameIds);
  if (error) throw error;

  const buckets = ['0-15', '16-30', '31-45', '46-60', '61-75', '76-90+'];
  const result = buckets.map(b => ({ bucket: b, scored: 0, conceded: 0 }));
  const bIdx = Object.fromEntries(buckets.map((b, i) => [b, i]));
  for (const g of goals || []) {
    const i = bIdx[g.goal_time_bucket] ?? 5;
    if (g.goal_for_team_id == teamId) result[i].scored++;
    else result[i].conceded++;
  }
  return result;
}

async function getTeamLeague(teamId) {
  const sb = getClient();
  const { data, error } = await sb.schema(SCHEMA).from('dim_teams')
    .select('league_id').eq('team_id', teamId).limit(1);
  if (error || !data.length) return null;
  return data[0].league_id;
}

async function fetchLeagueAverages(leagueId, seasonId) {
  const sb = getClient();
  let q = sb.schema(SCHEMA).from('fct_league_season')
    .select('total_team_games, total_goals_scored, total_goals_conceded, total_points');
  if (leagueId) q = q.eq('league_id', leagueId);
  if (seasonId) q = q.eq('season_id', seasonId);
  const { data, error } = await q;
  if (error) throw error;
  if (!data || !data.length) return null;
  const row = data[0];
  const n = row.total_team_games || 1;
  return {
    avg_pts: row.total_points / n,
    avg_gf:  row.total_goals_scored / n,
    avg_ga:  row.total_goals_conceded / n,
  };
}

async function fetchTeamPlayerMinutes(teamId, seasonId) {
  const sb = getClient();
  let q = sb.schema(SCHEMA).from('fct_player_stats')
    .select('player_id, minutes_total, apps_total, starts_total')
    .eq('team_id', teamId)
    .gt('minutes_total', 0)
    .order('minutes_total', { ascending: false });
  if (seasonId) q = q.eq('season_id', seasonId);
  const { data: stats, error: sErr } = await q;
  if (sErr) throw sErr;
  if (!stats || !stats.length) return [];
  const playerIds = stats.map(s => s.player_id);
  const { data: players, error: pErr } = await sb.schema(SCHEMA).from('dim_players')
    .select('player_id, firstname, lastname')
    .in('player_id', playerIds);
  if (pErr) throw pErr;
  const nameMap = Object.fromEntries((players || []).map(p => [p.player_id, `${p.firstname || ''} ${p.lastname || ''}`.trim()]));
  return stats.map(s => ({
    player_name: nameMap[s.player_id] || `#${s.player_id}`,
    minutes: s.minutes_total,
    apps: s.apps_total,
    starts: s.starts_total,
  }));
}

async function fetchLeagueGoalStats(leagueId, seasonId) {
  const sb = getClient();
  let q = sb.schema(SCHEMA).from('fct_league_season')
    .select('total_team_games, total_goals_scored, home_goals, away_goals');
  if (leagueId) q = q.eq('league_id', leagueId);
  if (seasonId) q = q.eq('season_id', seasonId);
  const { data, error } = await q;
  if (error) throw error;
  if (!data || !data.length) return { total: 0, home: 0, away: 0, avg_per_game: '-', avg_home_per_game: '-', avg_away_per_game: '-', games: 0 };
  const row = data[0];
  const totalGames = (row.total_team_games || 0) / 2;
  const fmt = v => totalGames > 0 ? (v / totalGames).toFixed(2) : '-';
  return {
    total: row.total_goals_scored,
    home:  row.home_goals,
    away:  row.away_goals,
    avg_per_game:      fmt(row.total_goals_scored),
    avg_home_per_game: fmt(row.home_goals),
    avg_away_per_game: fmt(row.away_goals),
    games: Math.round(totalGames),
  };
}

async function fetchTeamGoalsByType(teamId, seasonId, location) {
  const sb = getClient();
  let gamesQ = sb.schema(SCHEMA).from('fct_team_games')
    .select('game_id').eq('team_id', teamId);
  if (seasonId) gamesQ = gamesQ.eq('season_id', seasonId);
  if (location) gamesQ = gamesQ.eq('game_location', location);
  const { data: tgames, error: tgErr } = await gamesQ;
  if (tgErr) throw tgErr;
  const gameIds = (tgames || []).map(g => g.game_id);
  if (!gameIds.length) return [];

  const { data: goals, error } = await sb.schema(SCHEMA).from('fct_goals')
    .select('game_situation, goal_for_team_id, own_goal').in('game_id', gameIds);
  if (error) throw error;

  const situations = ['Open Play', 'Penalty', 'Free Kick'];
  const tally = {};
  for (const s of situations) tally[s] = { scored: 0, conceded: 0 };
  for (const g of goals || []) {
    const sit = g.game_situation || 'Open Play';
    if (!tally[sit]) tally[sit] = { scored: 0, conceded: 0 };
    if (g.goal_for_team_id == teamId) tally[sit].scored++;
    else tally[sit].conceded++;
  }
  return situations
    .filter(s => tally[s].scored + tally[s].conceded > 0)
    .map(s => ({ situation: s, scored: tally[s].scored, conceded: tally[s].conceded }));
}

async function fetchTeamStrengths(leagueId, seasonId) {
  const sb = getClient();
  let q = sb.schema(SCHEMA).from('fct_team_strength')
    .select('team_id, attack_rating, defence_rating, form_rating, form_pts_last_5, overall_strength, pyth_points, pyth_rank');
  if (leagueId) q = q.eq('league_id', leagueId);
  if (seasonId) q = q.eq('season_id', seasonId);
  const { data, error } = await q;
  if (error) throw error;
  return Object.fromEntries((data || []).map(r => [r.team_id, r]));
}

module.exports = { fetchLeagues, fetchSeasons, fetchTeams, fetchLeagueTable, fetchTopScorers, fetchLeagueGoalStats, fetchTeamGames, fetchTeamSeasonStats, fetchTeamPlayerMinutes, fetchTeamGoalsByType, fetchPlayers, fetchPlayerGames, fetchGameDetail, fetchTeamGoalsTiming, getTeamLeague, fetchLeagueAverages, fetchTeamStrengths, MY_TEAM_ID };
