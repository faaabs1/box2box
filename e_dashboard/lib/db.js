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

  // Fetch home and away games from raw schema (confirmed available)
  const [homeRes, awayRes] = await Promise.all([
    sb.schema('raw').from('games').select('game_id, game_date, game_round, home_team_id, away_team_id, home_goals, away_goals').eq('home_team_id', teamId),
    sb.schema('raw').from('games').select('game_id, game_date, game_round, home_team_id, away_team_id, home_goals, away_goals').eq('away_team_id', teamId),
  ]);
  if (homeRes.error) throw homeRes.error;
  if (awayRes.error) throw awayRes.error;

  const allTeams = await fetchTeams(null);
  const teamMap = Object.fromEntries(allTeams.map(t => [t.team_id, t]));

  // Season date filtering
  let seasonRange = null;
  if (seasonId) {
    const { data: seasons } = await sb.schema('raw').from('seasons').select('season_id, valid_from, valid_to').eq('season_id', seasonId);
    if (seasons && seasons.length) seasonRange = seasons[0];
  }

  const normalize = (row, isHome) => {
    const gs = isHome ? row.home_goals : row.away_goals;
    const gc = isHome ? row.away_goals : row.home_goals;
    const gd = gs - gc;
    const points = gs > gc ? 3 : gs === gc ? 1 : 0;
    const opponentId = isHome ? row.away_team_id : row.home_team_id;
    return {
      game_id: row.game_id,
      game_date: row.game_date,
      game_round: row.game_round,
      game_location: isHome ? 'home' : 'away',
      goals_scored: gs,
      goals_conceded: gc,
      goal_difference: gd,
      points,
      result: points === 3 ? 'W' : points === 1 ? 'D' : 'L',
      opponent_team_id: opponentId,
      opponent_name: teamMap[opponentId]?.team_name || `Team ${opponentId}`,
    };
  };

  let games = [
    ...homeRes.data.map(r => normalize(r, true)),
    ...awayRes.data.map(r => normalize(r, false)),
  ];

  if (seasonRange) {
    games = games.filter(g => g.game_date >= seasonRange.valid_from && g.game_date <= seasonRange.valid_to);
  }

  return games.sort((a, b) => (b.game_date || '').localeCompare(a.game_date || ''));
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

  // Use raw.players directly (confirmed columns: player_id, firstname, lastname,
  // position1, position2, strong_foot, jersey_number, dob, team_id)
  let playerQuery = sb.schema('raw').from('players')
    .select('player_id, firstname, lastname, position1, position2, strong_foot, jersey_number, dob, team_id')
    .order('jersey_number');
  if (teamId) playerQuery = playerQuery.eq('team_id', parseInt(teamId, 10));
  const { data: rawPlayers, error: rError } = await playerQuery;
  if (rError) throw rError;
  if (!rawPlayers.length) return [];

  const playerIds = rawPlayers.map(p => p.player_id);

  // Determine game IDs to scope stats to a season
  let gameIds = null;
  if (seasonId) {
    const { data: seasons } = await sb.schema('raw').from('seasons')
      .select('valid_from, valid_to').eq('season_id', seasonId);
    if (seasons && seasons.length) {
      const { valid_from, valid_to } = seasons[0];
      if (teamId) {
        const [hRes, aRes] = await Promise.all([
          sb.schema('raw').from('games').select('game_id').eq('home_team_id', parseInt(teamId, 10)).gte('game_date', valid_from).lte('game_date', valid_to),
          sb.schema('raw').from('games').select('game_id').eq('away_team_id', parseInt(teamId, 10)).gte('game_date', valid_from).lte('game_date', valid_to),
        ]);
        gameIds = [...(hRes.data || []), ...(aRes.data || [])].map(g => g.game_id);
      }
    }
  }

  // Lineups + goals for aggregation
  let lineupsQ = sb.schema('raw').from('lineups')
    .select('player_id, started, min_played, sub_in').in('player_id', playerIds);
  if (gameIds) lineupsQ = lineupsQ.in('game_id', gameIds);

  let goalsQ = sb.schema('raw').from('goals')
    .select('player_id').in('player_id', playerIds);
  if (gameIds) goalsQ = goalsQ.in('game_id', gameIds);

  const [lineupsRes, goalsRes] = await Promise.all([lineupsQ, goalsQ]);

  const statsByPlayer = {};
  for (const l of lineupsRes.data || []) {
    if (!statsByPlayer[l.player_id]) statsByPlayer[l.player_id] = { starts: 0, minutes: 0, subs_in: 0 };
    if (l.started) statsByPlayer[l.player_id].starts++;
    statsByPlayer[l.player_id].minutes += l.min_played || 0;
    if (!l.started && l.sub_in != null) statsByPlayer[l.player_id].subs_in++;
  }
  const goalsByPlayer = {};
  for (const g of goalsRes.data || []) goalsByPlayer[g.player_id] = (goalsByPlayer[g.player_id] || 0) + 1;

  return rawPlayers.map(p => {
    const s = statsByPlayer[p.player_id] || { starts: 0, minutes: 0, subs_in: 0 };
    const age = p.dob ? Math.floor((Date.now() - new Date(p.dob)) / (365.25 * 24 * 3600 * 1000)) : null;
    return {
      player_id: p.player_id,
      full_name: `${p.firstname || ''} ${p.lastname || ''}`.trim(),
      jersey_number: p.jersey_number,
      position1: p.position1,
      position2: p.position2,
      strong_foot: p.strong_foot,
      age,
      goals: goalsByPlayer[p.player_id] || 0,
      minutes: s.minutes,
      starts: s.starts,
      apps: s.starts + s.subs_in,
      subs_in: s.subs_in,
    };
  });
}

async function fetchPlayerGames(playerId) {
  const sb = getClient();
  const [lineupsRes, allTeams] = await Promise.all([
    sb.schema('raw').from('lineups')
      .select('game_id, team_id, started, min_played, sub_in, sub_out')
      .eq('player_id', playerId),
    fetchTeams(null),
  ]);
  if (lineupsRes.error) throw lineupsRes.error;
  if (!lineupsRes.data.length) return [];

  const gameIds = lineupsRes.data.map(l => l.game_id);
  const teamMap = Object.fromEntries(allTeams.map(t => [t.team_id, t]));

  const [gamesRes, goalsRes] = await Promise.all([
    sb.schema('raw').from('games')
      .select('game_id, game_date, game_round, home_team_id, away_team_id, home_goals, away_goals')
      .in('game_id', gameIds),
    sb.schema('raw').from('goals')
      .select('game_id').eq('player_id', playerId).in('game_id', gameIds),
  ]);
  if (gamesRes.error) throw gamesRes.error;

  const gameMap = Object.fromEntries((gamesRes.data || []).map(g => [g.game_id, g]));
  const goalsPerGame = {};
  for (const g of goalsRes.data || []) goalsPerGame[g.game_id] = (goalsPerGame[g.game_id] || 0) + 1;

  return lineupsRes.data.map(l => {
    const game = gameMap[l.game_id];
    if (!game) return null;
    const isHome = l.team_id == game.home_team_id;
    const gs = isHome ? game.home_goals : game.away_goals;
    const gc = isHome ? game.away_goals : game.home_goals;
    const points = gs > gc ? 3 : gs === gc ? 1 : 0;
    const opponentId = isHome ? game.away_team_id : game.home_team_id;
    return {
      game_id: l.game_id,
      game_date: game.game_date,
      game_round: game.game_round,
      game_location: isHome ? 'home' : 'away',
      started: l.started,
      min_played: l.min_played,
      sub_in_min: l.sub_in,
      sub_out_min: l.sub_out,
      goals: goalsPerGame[l.game_id] || 0,
      goals_scored: gs,
      goals_conceded: gc,
      result: points === 3 ? 'W' : points === 1 ? 'D' : 'L',
      opponent_name: teamMap[opponentId]?.team_name || `Team ${opponentId}`,
    };
  }).filter(Boolean).sort((a, b) => (b.game_date || '').localeCompare(a.game_date || ''));
}

async function fetchGameDetail(gameId) {
  const sb = getClient();
  const [gameRes, lineupsRes, goalsRes, allTeams] = await Promise.all([
    sb.schema('raw').from('games')
      .select('game_id, game_date, game_round, home_team_id, away_team_id, home_goals, away_goals')
      .eq('game_id', gameId),
    sb.schema('raw').from('lineups')
      .select('player_id, team_id, started, min_played, sub_in, sub_out')
      .eq('game_id', gameId),
    sb.schema('raw').from('goals')
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
    const { data: players } = await sb.schema('raw').from('players')
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
  const [homeRes, awayRes] = await Promise.all([
    sb.schema('raw').from('games').select('game_id, game_date').eq('home_team_id', teamId),
    sb.schema('raw').from('games').select('game_id, game_date').eq('away_team_id', teamId),
  ]);
  if (homeRes.error) throw homeRes.error;
  if (awayRes.error) throw awayRes.error;

  let allGames = [...homeRes.data, ...awayRes.data];
  if (seasonId) {
    const { data: seasons } = await sb.schema('raw').from('seasons')
      .select('valid_from, valid_to').eq('season_id', seasonId);
    if (seasons && seasons.length) {
      const { valid_from, valid_to } = seasons[0];
      allGames = allGames.filter(g => g.game_date >= valid_from && g.game_date <= valid_to);
    }
  }
  const gameIds = allGames.map(g => g.game_id);
  if (!gameIds.length) return [];

  const { data: goals, error } = await sb.schema('raw').from('goals')
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
  let seasonRange = null;
  if (seasonId) {
    const { data: seasons } = await sb.schema('raw').from('seasons')
      .select('valid_from, valid_to').eq('season_id', seasonId);
    if (seasons && seasons.length) seasonRange = seasons[0];
  }
  let homeQ = sb.schema('raw').from('games').select('home_team_id').in('home_team_id', teamIds);
  let awayQ = sb.schema('raw').from('games').select('away_team_id').in('away_team_id', teamIds);
  if (seasonRange) {
    homeQ = homeQ.gte('game_date', seasonRange.valid_from).lte('game_date', seasonRange.valid_to);
    awayQ = awayQ.gte('game_date', seasonRange.valid_from).lte('game_date', seasonRange.valid_to);
  }
  const [hgRes, agRes] = await Promise.all([homeQ, awayQ]);
  const gpMap = {};
  for (const g of hgRes.data || []) gpMap[g.home_team_id] = (gpMap[g.home_team_id] || 0) + 1;
  for (const g of agRes.data || []) gpMap[g.away_team_id] = (gpMap[g.away_team_id] || 0) + 1;

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
