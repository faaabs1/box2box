from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import psycopg2
from config import MY_TEAM_ID, GAME_SITUATIONS, CARD_REASONS

def connect_db():

    # Load environment variables from .env
    load_dotenv()

    # Fetch variables
    USER = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST = os.getenv("host")
    PORT = os.getenv("port")
    DBNAME = os.getenv("dbname")

    # Construct the SQLAlchemy connection string
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    # Create the SQLAlchemy engine
    #engine = create_engine(DATABASE_URL)
    engine = create_engine(DATABASE_URL, poolclass=NullPool)

    # Test the connection
    try:
        with engine.connect() as connection:
            print("Connection successful!")
    except Exception as e:
        print(f"Failed to connect: {e}")




def connect_db_api():
    
    # Load environment variables from .env
    load_dotenv()

    # Fetch variables
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Create the SQLAlchemy engine
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

def fetch_teams(connection, game_id=None):
    if game_id is None: 
        teams = connection.schema('raw').table('teams').select('team_id','team_name').execute()
    else:
        teams = connection.schema('raw').table('teams').select('team_id','team_name').eq('game_id', game_id).execute()
    teams_df = pd.DataFrame(teams.data)
    return teams_df

def fetch_players(connection, team_id=None):
    if team_id is None:
        players = connection.schema('raw').table('players').select('player_id','firstname','lastname').execute()
    else:
        players = connection.schema('analytics').table('active_rosters').select('player_id','firstname','lastname').eq('team_id',team_id).execute()
    players_df = pd.DataFrame(players.data)
    return players_df

def fetch_leagues(connection):
    leagues = connection.schema('raw').table('leagues').select('league_id','league_name').execute()
    leagues_df = pd.DataFrame(leagues.data)
    return leagues_df

def fetch_games(connection):
    games = connection.schema('raw').table('games').select('game_id','home_team_id','away_team_id').execute()
    games_df = pd.DataFrame(games.data)
    return games_df

def save_game(connection, league, game_date, game_time, round, home_team, away_team, home_goals, away_goals, game_attendance):
    game_data = {
        "game_date": game_date.isoformat(),
        "game_kickoff": game_time.strftime("%H:%M:%S"),
        'game_round': round,
        'home_team_id': home_team,
        'away_team_id': away_team,
        'home_goals': home_goals,
        'away_goals': away_goals,
        'game_attendance': game_attendance,
        'league_id': league,
    }
    response = connection.schema('raw').table('games').insert(game_data).execute()
    return response

def save_goal(connection, game_id, goal_minute, player_id = None, situation = None, own_goal = 0, goal_for=None):
    goal_data = {
        "game_id": game_id,
        "goal_min": goal_minute,
        "player_id": player_id,
        "game_situation": situation,
        "own_goal": own_goal,
        "goal_for": goal_for
    }
    response = connection.schema('raw').table('goals').insert(goal_data).execute()
    return response

def save_card(connection, game_id, player_id, minute, yc, rc, straight_rc):
    card_data = {
        "game_id": game_id,
        "player_id": player_id,
        "minute": minute,
        "yellow_card": yc,
        "red_card": rc,
        "straight_red_card": straight_rc
    }
    response = connection.schema('raw').table('cards').insert(card_data).execute()
    return response

def save_lineup(connection, game_id, player_id, minutes_played, sub_in, sub_out, is_starter,team_id):
    lineup_data = {
        "game_id": game_id,
        "player_id": player_id,
        "min_played": minutes_played,
        "sub_in": sub_in,
        "sub_out": sub_out,
        "started": is_starter,
        "team_id": team_id
    }
    response = connection.schema('raw').table('lineups').insert(lineup_data).execute()
    return response



def entry_game():
    print(fetch_leagues(sb))
    league = int(input("League: "))
    game_year = int(input("Year: "))
    game_month = int(input("Month: "))
    game_day = int(input("Game Day: "))
    game_date = dt.date(game_year,game_month,game_day)
    game_hour = int(input("Game Hour: "))
    game_min = int(input("Game Min: "))
    game_time = dt.time(game_hour,game_min)
    round = int(input("Round: "))
    print(fetch_teams(sb))
    home_team = int(input("Home Team: "))
    print(fetch_teams(sb))
    away_team = int(input("Away Team: "))
    home_goals = int(input("Home Goals: "))
    away_goals = int(input("Away Goals: "))
    game_attendance = int(input("Game Attendance: "))
    save_game(sb, league, game_date, game_time, round, home_team, away_team, home_goals, away_goals, game_attendance)
    return {home_team:home_goals, away_team:away_goals}

def max_game_id(connection):
    games = connection.schema('raw').table('games').select('game_id').execute()
    games_df = pd.DataFrame(games.data)
    max_game_id = games_df['game_id'].max()
    max_game_id = int(max_game_id)
    return max_game_id

def entry_goals(max_game_id,dict_game:dict):
    for team_id,goals in dict_game.items():
        if goals > 0:
            for i in range(goals):
                print(f"\n--- Entering {goals} goals for Team {team_id} ---")
                goal_for = team_id
                goal_min = int(input("Goal Minute: "))
                own_goal = int(input("Own Goal (1/0): "))
                situation = GAME_SITUATIONS[int(input(f"Game Situation: "))]
                if own_goal == 0 and team_id == MY_TEAM_ID:
                    print(fetch_players(sb, team_id))
                    try:
                        player_id = int(input(f"Player ID for Team {team_id}: "))
                    except ValueError:
                        print("Invalid input, saving as None.")
                        player_id = int(input(f"Player ID for Team {team_id}: "))
                    save_goal(sb, max_game_id, goal_min, player_id,situation,own_goal, goal_for)
                    print("Goals saved!")
                else:
                    player_id = None
                    save_goal(sb, max_game_id, goal_min,player_id,situation,own_goal, goal_for)
                    print("Goals saved!")


def entry_cards(game_id):
    cards = int(input("Number of Cards to enter: "))
    for i in range(cards):
        team_id = MY_TEAM_ID
        print(fetch_players(sb, team_id))
        player_id = int(input("Player ID: "))
        minute = int(input("Minute: "))
        yc = int(input("Yellow Card (1/0): "))
        if yc == 1: 
            rc = 0
            straigt_rc = 0
        else:
            yc_reason = CARD_REASONS[int(input("Yellow Card Reason: "))]
            rc = int(input("Red Card (1/0): "))
            if rc == 1:
                straigt_rc = int(input("Straight Red Card (1/0): "))
            else:
                straigt_rc = 0
        #rc = int(input("Red Card (1/0): "))
        #straigt_rc = int(input("Straight Red Card (1/0): "))
        save_card(sb, game_id, player_id, minute, yc, rc, straigt_rc)


def entry_lineup(game_id):
    print("--- Entering Starting Lineup ---")
    subs = 0
    for i in range(11):
        team_id = MY_TEAM_ID
        print(fetch_players(sb,team_id))
        player_id = int(input("Player ID: "))
        sub_in = 0
        sub_out = int(input("Sub Out (1/0): "))
        if sub_out == 1:
            subs+=1
        if sub_in == 0:
            is_starter = 1
        else:
            is_starter = 0
        if is_starter == 1 and sub_out == 0:
            minutes_played = 90
        elif is_starter == 1 and sub_out == 1:
            minutes_played = int(input("Minutes Played: "))
        elif is_starter == 0 and sub_in == 1:
            sub_in_min = int(input("Sub In Minute: "))
            minutes_played = 90 - sub_in_min
        elif is_starter == 0 and sub_in == 1 and sub_out == 1:
            sub_in_min = int(input("Sub In Minute: "))
            sub_out_min = int(input("Sub Out Minute: "))
            minutes_played = sub_out_min - sub_in_min
        print(f'Player {i} added.')
        save_lineup(sb, game_id, player_id, minutes_played, sub_in, sub_out, is_starter,team_id)
    
    sub_in_out = 0
    print(f"--- Entering Substitutions: {subs} ---")
    for _ in range(subs):
        team_id = MY_TEAM_ID
        print(fetch_players(sb,team_id))
        player_id = int(input("Player ID: "))
        is_starter = 0
        sub_in = 1
        sub_in_min = int(input("Sub In Minute: "))
        sub_out = int(input("Sub Out (1/0): "))
        if sub_out == 1:
            sub_out_min = int(input("Sub Out Minute: "))
            minutes_played = sub_out_min - sub_in_min
            sub_in_out +1
        else:
            minutes_played = 90 - sub_in_min
        print(f'Player substitution added.')
        save_lineup(sb, game_id, player_id, minutes_played, sub_in, sub_out, is_starter,team_id)
    
    print(f"--- Entering {sub_in_out} Substitutions In/Out ---")
    for _ in range(sub_in_out):
        team_id = MY_TEAM_ID
        print(fetch_players(sb,team_id))
        player_id = int(input("Player ID: "))
        is_starter = 0
        sub_in = 1
        sub_in_min = int(input("Sub In Minute: "))
        sub_out = 1
        sub_out_min = int(input("Sub Out Minute: "))
        minutes_played = sub_out_min - sub_in_min
        print(f'Player substitution in/out added.')
        save_lineup(sb, game_id, player_id, minutes_played, sub_in, sub_out, is_starter,team_id)


if __name__ == "__main__":
    sb = connect_db_api()
    #sb = connect_db()
    dict_game = entry_game()
    current_game_id = max_game_id(sb)
    teams_playing = list(dict_game.keys())
    entry_goals(current_game_id, dict_game)
    if MY_TEAM_ID in teams_playing:
        print(f"My Team ({MY_TEAM_ID}) played! Entering details...")
        entry_lineup(current_game_id) 
        #entry_cards(current_game_id)
    else:
        print("My Team did not play. Skipping details.")