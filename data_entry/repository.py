# repository.py
import pandas as pd
from config import MY_TEAM_ID

class FootballRepository:
    def __init__(self, db_client):
        self.sb = db_client

    # --- FETCH METHODS ---
    def fetch_leagues(self):
        response = self.sb.schema('raw').table('leagues').select('league_id, league_name').execute()
        return pd.DataFrame(response.data)

    def fetch_teams(self, league_id, game_id=None):
        query = self.sb.schema('analytics').table('active_league').select('team_id, team_name').eq('league_id', league_id)
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
    
    def fetch_team_goals(self):
        response = self.sb.schema('analytics').table('team_goals').select('*').execute()
        return pd.DataFrame(response.data)
    
    def fetch_max_playerid(self):
        response = self.sb.schema('raw').table('players') \
            .select('player_id, firstname, lastname') \
            .order('player_id', desc=True) \
            .limit(1) \
            .execute()
        if response.data:
            return int(response.data[0]['player_id'])
        return 0
    
    def fetch_games_myteam(self,team_id=MY_TEAM_ID):
        response = self.sb.schema('raw').table('games').select('*').eq('home_team_id' or 'away_team_id', team_id).execute()
        return pd.DataFrame(response.data)
    
    def fetch_goal_ids(self,team_id):
    
    def get_max_game_id(self):
        response = self.sb.schema('raw').table('games') \
            .select('game_id').order('game_id', desc=True).limit(1).execute()
        if response.data:
            return int(response.data[0]['game_id'])
        return 0
    
    def fetch_card_ids(self,team_id):
        response = self.sb.schema('raw').table('cards').select('card_id','card_min').eq('team_id',team_id).execute()
        return pd.DataFrame(response.data)

    def fetch_player_stats(self):
        response = self.sb.schema('analytics').table('player_info')\
        .select('player_name','minutes_total','starts_total','goals_total','position1','jersey_number')\
        .order('jersey_number',desc=False)\
        .execute()
        return pd.DataFrame(response.data)
    
    def fetch_active_leauges(self):
        response = self.sb.schema('analytics').table('active_league').select('team_name','league_id').execute()
        return pd.DataFrame(response.data)

    # --- SAVE METHODS ---
    def save_game(self, payload):
        return self.sb.schema('raw').table('games').insert(payload).execute()

    def save_goal(self, payload):
        return self.sb.schema('raw').table('goals').insert(payload).execute()

    def save_card(self, payload):
        return self.sb.schema('raw').table('cards').insert(payload).execute()

    def save_lineup(self, payload):
        return self.sb.schema('raw').table('lineups').insert(payload).execute()
    
    def save_player_to_player(self, payload):
        return self.sb.schema('raw').table('players').insert(payload).execute()
    
    def save_player_contract(self, payload):
        return self.sb.schema('raw').table('player_contracts').insert(payload).execute()
    
    def save_xmistake(self, payload):
        return self.sb.schema('raw').table('xmistake').insert(payload).execute()
    

    # --- CREATE METHODS ---
    def create_new_player(self,team_id):
        #player table
        firstname = input("First Name: ")
        lastname = input("Last Name: ")
       
       #player_contract 
        valid_from = input("Contract Valid From (YYYY-MM-DD): ")

        # Prepare Payload
        payload_player = {
            "firstname": firstname,
            "lastname": lastname
        }
        self.save_player_to_player(payload_player)

        # Prepare Payload for Player Contract
        payload_contract = {
            "player_id": self.fetch_max_playerid(),
            "team_id": team_id,
            "valid_from": valid_from
        }
        self.save_player_contract(payload_contract)