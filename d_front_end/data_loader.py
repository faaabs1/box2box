import pandas as pd

class DataLoader:
    def __init__(self, db_client):
        self.sb = db_client
        self.analytics_schema = 'analytics_analytics'
    
    def fetch_leagues(self):
        response = self.sb.schema(self.analytics_schema).table('dim_leagues').select('league_id, league_name').execute()
        return pd.DataFrame(response.data)
    
    def fetch_seasons(self):
        response = self.sb.schema(self.analytics_schema).table('dim_seasons').select('season_id, season_name').execute()
        return pd.DataFrame(response.data)
    
    def fetch_teams(self, league_id,season_id=None):
        response = self.sb.schema(self.analytics_schema).table('dim_teams').select('team_id, team_name').eq('league_id', league_id).execute()
        return pd.DataFrame(response.data)
    
    def fetch_team_stats(self,league_id,season_id,team_id):
        response = self.sb.schema(self.analytics_schema).table('fct_teams_season').select('*').eq('league_id', league_id).eq('season_id', season_id).eq('team_id', team_id).execute()
        return pd.DataFrame(response.data)