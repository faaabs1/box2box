# main.py
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository
from data_entry.cli import MatchEntryCLI

if __name__ == "__main__":
    try:
        print("Initializing Box2Box...")
        
        # 1. Connect
        db = DatabaseClient()
        
        # 2. Setup Data Layer
        repo = FootballRepository(db.get_client())
        
        # 3. Start App
        app = MatchEntryCLI(repo)
        app.run()
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")