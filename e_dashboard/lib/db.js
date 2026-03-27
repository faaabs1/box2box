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

async function fetchLeagueTable(leagueId, seasonId) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_teams_season')
    .select('team_id, league_id, season_id, game_location, total_goals_scored, total_goals_conceded, total_goal_difference, total_points, total_points_allowed');
  if (leagueId) query = query.eq('league_id', leagueId);
  if (seasonId) query = query.eq('season_id', seasonId);
  const { data, error } = await query;
  if (error) throw error;

  // Aggregate home + away per team
  const byTeam = {};
  for (const row of data) {
    if (!byTeam[row.team_id]) {
      byTeam[row.team_id] = { team_id: row.team_id, league_id: row.league_id, season_id: row.season_id, goals_scored: 0, goals_conceded: 0, goal_difference: 0, points: 0, games_played: 0 };
    }
    byTeam[row.team_id].goals_scored += row.total_goals_scored || 0;
    byTeam[row.team_id].goals_conceded += row.total_goals_conceded || 0;
    byTeam[row.team_id].goal_difference += row.total_goal_difference || 0;
    byTeam[row.team_id].points += row.total_points || 0;
  }

  // Attach team names
  const teams = await fetchTeams(leagueId);
  const teamMap = Object.fromEntries(teams.map(t => [t.team_id, t]));

  return Object.values(byTeam)
    .map(t => ({ ...t, team_name: teamMap[t.team_id]?.team_name || `Team ${t.team_id}`, team_abb: teamMap[t.team_id]?.team_abb }))
    .sort((a, b) => b.points - a.points || b.goal_difference - a.goal_difference || b.goals_scored - a.goals_scored);
}

async function fetchTeamGames(teamId, seasonId, leagueId) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_team_games')
    .select('game_id, game_date, game_round, game_location, goals_scored, goals_conceded, goal_difference, points, result, opponent_team_id, opponent_name')
    .eq('team_id', teamId);
  if (seasonId) query = query.eq('season_id', seasonId);
  if (leagueId) query = query.eq('league_id', leagueId);
  const { data, error } = await query;
  if (error) throw error;
  return (data || []).sort((a, b) => (b.game_date || '').localeCompare(a.game_date || ''));
}

async function fetchTeamSeasonStats(teamId, seasonId, leagueId) {
  const sb = getClient();
  let query = sb.schema(SCHEMA).from('fct_teams_season')
    .select('*')
    .eq('team_id', teamId);
  if (seasonId) query = query.eq('season_id', seasonId);
  if (leagueId) query = query.eq('league_id', leagueId);
  const { data, error } = await query;
  if (error) throw error;

  // Aggregate home + away
  const totals = { wins: 0, draws: 0, losses: 0, goals_scored: 0, goals_conceded: 0, goal_difference: 0, points: 0, games_played: 0 };
  for (const row of data) {
    totals.goals_scored += row.total_goals_scored || 0;
    totals.goals_conceded += row.total_goals_conceded || 0;
    totals.goal_difference += row.total_goal_difference || 0;
    totals.points += row.total_points || 0;
  }
  return totals;
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

async function fetchTeamGoalsTiming(teamId, seasonId) {
  const sb = getClient();
  let gamesQ = sb.schema(SCHEMA).from('fct_team_games')
    .select('game_id').eq('team_id', teamId);
  if (seasonId) gamesQ = gamesQ.eq('season_id', seasonId);
  const { data: tgames, error: tgErr } = await gamesQ;
  if (tgErr) throw tgErr;

  const gameIds = (tgames || []).map(g => g.game_id);
  if (!gameIds.length) return [];

  const { data: goals, error } = await sb.schema(SCHEMA).from('fct_goals')
    .select('goal_min, goal_for_team_id').in('game_id', gameIds);
  if (error) throw error;

  const buckets = ['0-15', '16-30', '31-45', '46-60', '61-75', '76-90+'];
  const result = buckets.map(b => ({ bucket: b, scored: 0, conceded: 0 }));
  const bucketIdx = m => m <= 15 ? 0 : m <= 30 ? 1 : m <= 45 ? 2 : m <= 60 ? 3 : m <= 75 ? 4 : 5;
  for (const g of goals || []) {
    const i = bucketIdx(g.goal_min || 0);
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
  const teams = await fetchTeams(leagueId);
  if (!teams.length) return null;

  let statsQuery = sb.schema(SCHEMA).from('fct_teams_season')
    .select('team_id, total_goals_scored, total_goals_conceded, total_points');
  if (leagueId) statsQuery = statsQuery.eq('league_id', leagueId);
  if (seasonId) statsQuery = statsQuery.eq('season_id', seasonId);
  const { data: stats, error } = await statsQuery;
  if (error) throw error;

  const byTeam = {};
  for (const row of stats || []) {
    if (!byTeam[row.team_id]) byTeam[row.team_id] = { gf: 0, ga: 0, pts: 0 };
    byTeam[row.team_id].gf += row.total_goals_scored || 0;
    byTeam[row.team_id].ga += row.total_goals_conceded || 0;
    byTeam[row.team_id].pts += row.total_points || 0;
  }

  const teamIds = teams.map(t => t.team_id);
  let gpQuery = sb.schema(SCHEMA).from('fct_team_games')
    .select('team_id').in('team_id', teamIds);
  if (seasonId) gpQuery = gpQuery.eq('season_id', seasonId);
  if (leagueId) gpQuery = gpQuery.eq('league_id', leagueId);
  const { data: gpData, error: gpErr } = await gpQuery;
  if (gpErr) throw gpErr;
  const gpMap = {};
  for (const g of gpData || []) gpMap[g.team_id] = (gpMap[g.team_id] || 0) + 1;

  const avgs = teams.map(t => {
    const s = byTeam[t.team_id] || { gf: 0, ga: 0, pts: 0 };
    const gp = gpMap[t.team_id] || 1;
    return { avg_pts: s.pts / gp, avg_gf: s.gf / gp, avg_ga: s.ga / gp };
  });
  const n = avgs.length || 1;
  return {
    avg_pts: avgs.reduce((s, t) => s + t.avg_pts, 0) / n,
    avg_gf: avgs.reduce((s, t) => s + t.avg_gf, 0) / n,
    avg_ga: avgs.reduce((s, t) => s + t.avg_ga, 0) / n,
  };
}

module.exports = { fetchLeagues, fetchSeasons, fetchTeams, fetchLeagueTable, fetchTeamGames, fetchTeamSeasonStats, fetchPlayers, fetchPlayerGames, fetchGameDetail, fetchTeamGoalsTiming, getTeamLeague, fetchLeagueAverages, MY_TEAM_ID };
