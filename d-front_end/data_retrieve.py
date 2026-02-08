

class FootballRepository:
    def __init__(self, db_client):
        self.sb = db_client

    def fetch_leagues(self):
        query = "SELECT league_id, league_name FROM analytics_analytics.dim_leagues"
        return self.sb.query(query)
    
    def fetch_teams(self, league_id):
        query = f"""
        SELECT team_id, team_name, team_abb
        FROM analytics_analytics.dim_teams
        WHERE league_id = {league_id}
        """
        return self.sb.query(query)
    
    def fetch_teams_season(self,league_id,season_id):
        query = f"""
        SELECT ts.team_id, dt.team_name, dt.team_abb,ts.total_goals_scored, ts.total_goals_conceded, ts.total_points
        FROM analytics_analytics.fct_teams_season ts
        JOIN analytics_analytics.dim_teams dt USING (team_id)
        WHERE league_id = {league_id} AND season_id = {season_id}
        """
        return self.sb.query(query)
    