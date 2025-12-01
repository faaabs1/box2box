# repository.py
import pandas as pd

class FootballRepository:
    def __init__(self, db_client):
        self.sb = db_client

    # --- FETCH METHODS ---
    def fetch_leagues(self):
        response = self.sb.schema('raw').table('leagues').select('league_id, league_name').execute()
        return pd.DataFrame(response.data)

    def fetch_teams(self, game_id=None):
        query = self.sb.schema('raw').table('teams').select('team_id, team_name')
        if game_id:
            query = query.eq('game_id', game_id)
        response = query.execute()
        return pd.DataFrame(response.data)

    def fetch_roster(self, team_id=None):
        response = self.sb.schema('raw').table('players').select('player_id, firstname, lastname').execute()
        if team_id:
            response = self.sb.schema('analytics').table('active_rosters') \
                .select('player_id, firstname, lastname') \
                .eq('team_id', team_id).execute()
        return pd.DataFrame(response.data)

    def get_max_game_id(self):
        response = self.sb.schema('raw').table('games') \
            .select('game_id').order('game_id', desc=True).limit(1).execute()
        if response.data:
            return int(response.data[0]['game_id'])
        return 0

    # --- SAVE METHODS ---
    def save_game(self, payload):
        return self.sb.schema('raw').table('games').insert(payload).execute()

    def save_goal(self, payload):
        return self.sb.schema('raw').table('goals').insert(payload).execute()

    def save_card(self, payload):
        return self.sb.schema('raw').table('cards').insert(payload).execute()

    def save_lineup(self, payload):
        return self.sb.schema('raw').table('lineups').insert(payload).execute()