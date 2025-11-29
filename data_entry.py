from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
import datetime as dt

def connect_db():
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

def fetch_teams(connection, option:bool, game_id=None):
    if option == True:
        teams = connection.schema('raw').table('teams').select('team_id','team_name').execute()
    else:
        teams = connection.schema('raw').table('teams').select('team_id','team_name').eq('game_id', game_id).execute()
    teams_df = pd.DataFrame(teams.data)
    return teams_df

def fetch_players(connection,opion:bool, team_id=None):
    if opion == True:
        players = connection.schema('raw').table('players').select('player_id','player_name').execute()
    else:
        players = connection.schema('raw').table('players').select('player_id','player_name').eq('team_id', team_id).execute()
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



def entry_game():
    print(fetch_leagues(sb))
    league = input("League: ")
    game_year = int(input("Year: "))
    game_month = int(input("Month: "))
    game_day = int(input("Game Day: "))
    game_date = dt.date(game_year,game_month,game_day)
    game_hour = int(input("Game Hour: "))
    game_min = int(input("Game Min: "))
    game_time = dt.time(game_hour,game_min)
    round = int(input("Round: "))
    print(fetch_teams(sb, True))
    home_team = input("Home Team: ")
    print(fetch_teams(sb, True))
    away_team = input("Away Team: ")
    home_goals = int(input("Home Goals: "))
    away_goals = int(input("Away Goals: "))
    game_attendance = int(input("Game Attendance: "))
    save_game(sb, league, game_date, game_time, round, home_team, away_team, home_goals, away_goals, game_attendance)
    return home_team, away_team, home_goals, away_goals

def max_game_id(connection):
    games = connection.schema('raw').table('games').select('game_id').execute()
    games_df = pd.DataFrame(games.data)
    max_game_id = games_df['game_id'].max()
    return max_game_id

def entry_goals(max_game_id):
    #game_id = max_game_id
    print(fetch_teams(sb, False, max_game_id))
    goal_for = int(input("Goal For (1=Home, 0=Away): "))
    goal_min = int(input("Goal Minute: "))
    own_goal = int(input("Own Goal (1/0): "))
    if own_goal == 0:
        print(fetch_players(sb))
        player_id = int(input("Player ID: "))
    goal_minute = int(input("Goal Minute: "))


def entry_cards():
    print(fetch_games(sb))
    game_id = int(input("Game ID: "))
    player_id = int(input("Player ID: "))
    minute = int(input("Minute: "))
    yc = int(input("Yellow Card (1/0): "))
    rc = int(input("Red Card (1/0): "))
    straigt_rc = int(input("Straight Red Card (1/0): "))

def enry_lineup():
    print(fetch_games(sb))
    game_id = int(input("Game ID: "))
    print(fetch_teams(sb, False, game_id))
    team_id = int(input("Team ID: "))

    player_id = int(input("Player ID: "))
    sub_in = int(input("Sub In (1/0): "))
    sub_out = int(input("Sub Out (1/0): ")) 
    is_starter = int(input("Is Starter (1/0): "))
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


if __name__ == "__main__":
    sb = connect_db()
    home_team, away_team, home_goals, away_goals = entry_game()
    if (home_team == 1 or away_team == 1):
        for goals in range(home_goals):
            entry_goals(max_game_id(sb))
        for goals in range(away_goals):
            entry_goals(max_game_id(sb))
        entry_cards(max_game_id(sb))
        enry_lineup(max_game_id(sb))