const express = require('express');
const router = express.Router();
const db = require('../lib/db');

router.get('/leagues', async (req, res) => {
  try {
    res.json(await db.fetchLeagues());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/seasons', async (req, res) => {
  try {
    res.json(await db.fetchSeasons());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/teams', async (req, res) => {
  try {
    res.json(await db.fetchTeams(req.query.league_id || null));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/league-table', async (req, res) => {
  try {
    const { league_id, season_id } = req.query;
    res.json(await db.fetchLeagueTable(league_id || null, season_id || null));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/team-games', async (req, res) => {
  try {
    const { team_id, season_id, league_id } = req.query;
    if (!team_id) return res.status(400).json({ error: 'team_id required' });
    res.json(await db.fetchTeamGames(team_id, season_id || null, league_id || null));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/team-stats', async (req, res) => {
  try {
    const { team_id, season_id, league_id } = req.query;
    if (!team_id) return res.status(400).json({ error: 'team_id required' });
    res.json(await db.fetchTeamSeasonStats(team_id, season_id || null, league_id || null));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/players', async (req, res) => {
  try {
    const { team_id, season_id } = req.query;
    res.json(await db.fetchPlayers(team_id || null, season_id || null));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/player-games/:playerId', async (req, res) => {
  try {
    res.json(await db.fetchPlayerGames(req.params.playerId));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/game/:gameId', async (req, res) => {
  try {
    res.json(await db.fetchGameDetail(req.params.gameId));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
