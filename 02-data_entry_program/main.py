# main.py
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository
from data_entry.cli import MatchEntryCLI, GoalkeeperEntryCLI

if __name__ == "__main__":
    try:
        print("Initializing Box2Box...")
        
        # 1. Connect
        db = DatabaseClient()
        
        # 2. Setup Data Layer
        repo = FootballRepository(db.get_client())
        nav = int(input("Enter 1 for Data Entry CLI: "))
        
        if nav == 1:
            # 3. Start App
            app = MatchEntryCLI(repo)
            #app.run()
        if nav == 2:
            app = GoalkeeperEntryCLI(repo)
            
        app.run()
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")