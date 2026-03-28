const express = require('express');
const router = express.Router();
const db = require('../lib/db');

router.get('/', async (req, res) => {
  try {
    const [leagues, seasons] = await Promise.all([db.fetchLeagues(), db.fetchSeasons()]);
    const leagueId = req.query.league_id || (leagues[0]?.league_id ?? null);
    const seasonId = req.query.season_id || (seasons[0]?.season_id ?? null);
    const location = req.query.location || 'all';
    const [table, teams, topScorers, leagueGoalStats, strengthMap] = await Promise.all([
      db.fetchLeagueTable(leagueId, seasonId, location === 'all' ? null : location),
      db.fetchTeams(leagueId),
      db.fetchTopScorers(leagueId, seasonId),
      db.fetchLeagueGoalStats(leagueId, seasonId),
      db.fetchTeamStrengths(leagueId, seasonId),
    ]);
    const tableWithStrength = table.map(t => ({ ...t, ...(strengthMap[t.team_id] || {}) }));
    res.render('league', { leagues, seasons, table: tableWithStrength, teams, topScorers, leagueGoalStats, selectedLeague: leagueId, selectedSeason: seasonId, selectedLocation: location, myTeamId: db.MY_TEAM_ID, page: 'league' });
  } catch (e) {
    res.status(500).render('error', { message: e.message });
  }
});

router.get('/team', async (req, res) => {
  try {
    const [leagues, seasons, allTeams] = await Promise.all([db.fetchLeagues(), db.fetchSeasons(), db.fetchTeams(null)]);
    const teamId = parseInt(req.query.team_id || db.MY_TEAM_ID, 10);
    const seasonId = req.query.season_id || (seasons[0]?.season_id ?? null);
    let leagueId = req.query.league_id || null;
    const location = req.query.location || null;
    // Auto-detect league for this team if not provided
    if (!leagueId) leagueId = await db.getTeamLeague(teamId);
    const [games, stats, timing, leagueAvgs, teamScorers, playerMinutes, goalsByType] = await Promise.all([
      db.fetchTeamGames(teamId, seasonId, leagueId, location),
      db.fetchTeamSeasonStats(teamId, seasonId, leagueId, location),
      db.fetchTeamGoalsTiming(teamId, seasonId, location),
      db.fetchLeagueAverages(leagueId, seasonId),
      db.fetchTopScorers(null, seasonId, 3, teamId),
      db.fetchTeamPlayerMinutes(teamId, seasonId),
      db.fetchTeamGoalsByType(teamId, seasonId, location),
    ]);
    const selectedTeam = allTeams.find(t => t.team_id === teamId);
    const gamesPlayed = games.length || 1;
    res.render('team', { leagues, seasons, allTeams, games, stats, timing, leagueAvgs, gamesPlayed, teamScorers, playerMinutes, goalsByType, selectedTeam, selectedTeamId: teamId, selectedSeason: seasonId, selectedLeague: leagueId, selectedLocation: location, myTeamId: db.MY_TEAM_ID, page: 'team' });
  } catch (e) {
    res.status(500).render('error', { message: e.message });
  }
});

router.get('/game/:gameId', async (req, res) => {
  try {
    const [leagues, seasons] = await Promise.all([db.fetchLeagues(), db.fetchSeasons()]);
    const detail = await db.fetchGameDetail(req.params.gameId);
    res.render('game', { detail, leagues, seasons, myTeamId: db.MY_TEAM_ID, page: '' });
  } catch (e) {
    res.status(500).render('error', { message: e.message });
  }
});

router.get('/players', async (req, res) => {
  try {
    const [leagues, seasons, allTeams] = await Promise.all([db.fetchLeagues(), db.fetchSeasons(), db.fetchTeams(null)]);
    const teamId = req.query.team_id || db.MY_TEAM_ID;
    const seasonId = req.query.season_id || (seasons[0]?.season_id ?? null);
    const players = await db.fetchPlayers(teamId, seasonId);
    const selectedTeam = allTeams.find(t => t.team_id === parseInt(teamId, 10));
    res.render('players', { leagues, seasons, allTeams, players, selectedTeam, selectedTeamId: teamId, selectedSeason: seasonId, myTeamId: db.MY_TEAM_ID, page: 'players' });
  } catch (e) {
    res.status(500).render('error', { message: e.message });
  }
});

module.exports = router;
