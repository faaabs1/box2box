# cli.py
import datetime as dt
from config import MY_TEAM_ID, GAME_SITUATIONS 

# Helper function can live here or in a utils.py
def get_int(prompt):
    while True:
        try:
            return int(input(prompt))
        except ValueError:
            print("Invalid number.")

class MatchEntryCLI:
    def __init__(self, repo):
        self.repo = repo
        self.current_game_id = None
        self.teams_involved = {} # {id: goals}

    def run(self):
        print("Starting Box2Box Data Entry...")
        
        # 1. Game Info
        self.entry_game_details()
        
        # 2. Get the ID generated
        self.current_game_id = self.repo.get_max_game_id()
        print(f"Game saved. New ID: {self.current_game_id}")

        # 3. Goals
        self.entry_goals()

        # 4. Check if My Team played
        if MY_TEAM_ID in self.teams_involved:
            print(f"My Team ({MY_TEAM_ID}) played. Entering details...")
            self.entry_lineup()
            # self.entry_cards() # Uncomment when ready
        else:
            print("My Team did not play. Skipping lineup/cards.")

    def entry_game_details(self):
        print(self.repo.fetch_leagues())
        league = get_int("League ID: ")
        
        year = get_int("Year: ")
        month = get_int("Month: ")
        day = get_int("Day: ")
        game_date = dt.date(year, month, day)
        
        hour = get_int("Hour: ")
        minute = get_int("Minute: ")
        game_time = dt.time(hour, minute)
        
        round_num = get_int("Round: ")
        
        print(self.repo.fetch_teams())
        home_team = get_int("Home Team ID: ")
        away_team = get_int("Away Team ID: ")
        
        home_goals = get_int("Home Goals: ")
        away_goals = get_int("Away Goals: ")
        attendance = get_int("Attendance: ")

        # Save to state
        self.teams_involved = {home_team: home_goals, away_team: away_goals}

        # Prepare Payload
        payload = {
            "game_date": game_date.isoformat(),
            "game_kickoff": game_time.strftime("%H:%M:%S"),
            'game_round': round_num,
            'home_team_id': home_team,
            'away_team_id': away_team,
            'home_goals': home_goals,
            'away_goals': away_goals,
            'game_attendance': attendance,
            'league_id': league,
        }
        self.repo.save_game(payload)

    def entry_goals(self):
        for team_id, score in self.teams_involved.items():
            if score == 0: continue
            
            print(f"\n--- Entering {score} goals for Team {team_id} ---")
            for i in range(score):
                print(f"Goal {i+1} / {score}")
                goal_min = get_int("Goal Minute: ")
                own_goal = get_int("Own Goal (1/0): ")
                
                # Situation Selection
                print("Situations:", GAME_SITUATIONS)
                sit_idx = get_int("Situation Index: ")
                situation = GAME_SITUATIONS.get(sit_idx, "Normal")

                player_id = None
                # Only ask for player if it's My Team and NOT an own goal
                if own_goal == 0 and team_id == MY_TEAM_ID:
                    print(self.repo.fetch_roster(team_id))
                    try:
                        player_id = get_int(f"Player ID for Team {team_id}: ")
                    except ValueError:
                        player_id = None
                
                payload = {
                    "game_id": self.current_game_id,
                    "goal_min": goal_min,
                    "player_id": player_id,
                    "game_situation": situation,
                    "own_goal": own_goal,
                    "goal_for": team_id
                }
                self.repo.save_goal(payload)

    def entry_lineup(self):
        print("\n--- Entering Lineup for My Team ---")
        
        # Fetch Roster ONCE for the whole loop (Performance)
        roster_df = self.repo.fetch_roster(MY_TEAM_ID)
        print(roster_df)

        starters_count = 11
        subs_count = 0
        for i in range(starters_count):
            print(f"\nStarter {i+1}/{starters_count}")
            player_id = get_int("Player ID: ")
            
            sub_out = get_int("Sub Out? (1=Yes, 0=No): ")
            
            # Logic Calculation
            is_starter = 1
            sub_in = 0
            
            if sub_out == 1:
                sub_out_min = get_int("Sub Out Minute: ")
                min_played = sub_out_min
                subs_count+=1
            else:
                min_played = 90

            self._save_lineup_entry(player_id, min_played, sub_in, sub_out, is_starter)

        # Subs Logic (Simplified)
        sub_in_out_count = 0
        for _ in range(subs_count):
            print(roster_df)
            player_id = get_int("Sub Player ID: ")
            
            sub_in_min = get_int("Minute In: ")
            sub_out = get_int("Sub Out later? (1/0): ")
            
            is_starter = 0
            sub_in = 1
            
            if sub_out == 1:
                sub_out_min = get_int("Minute Out: ")
                min_played = sub_out_min - sub_in_min
                sub_in_out_count += 1
            else:
                min_played = 90 - sub_in_min
                
            self._save_lineup_entry(player_id, min_played, sub_in, sub_out, is_starter)

        for _ in range(sub_in_out_count):
            print(roster_df)
            player_id = get_int("Sub Player ID: ")
            is_starter = 0
            sub_in = 1
            sub_in_min = get_int("Minute In: ")
            sub_out = 1
            sub_out_min = get_int("Minute Out: ")
            min_played = sub_out_min - sub_in_min
            #print(f'Player substitution in/out added.')

            self._save_lineup(player_id, min_played, sub_in, sub_out, is_starter)

    def _save_lineup_entry(self, pid, mins, sin, sout, starter):
        payload = {
            "game_id": self.current_game_id,
            "player_id": pid,
            "min_played": mins,
            "sub_in": sin,
            "sub_out": sout,
            "started": starter,
            "team_id": MY_TEAM_ID
        }
        self.repo.save_lineup(payload)
        print("Lineup entry saved.")