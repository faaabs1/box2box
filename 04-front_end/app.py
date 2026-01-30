import sys
import os
from numpy import percentile

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) 
sys.path.append(parent_dir)

import streamlit as st
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository

# 1. Page Config
st.set_page_config(page_title="Box2Box Analytics", page_icon="⚽", layout="wide")

# 2. Connection Setup
@st.cache_resource
def get_repo():
    try:
        db = DatabaseClient()
        return FootballRepository(db.get_client())
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        return None

repo = get_repo()

if not repo:
    st.stop()

# 3. Load Data
@st.cache_data
def load_active_leagues():
    active_leagues = repo.fetch_active_leauges()
    return active_leagues
def load_leagues():
    leagues = repo.fetch_leagues()
    return leagues



al = load_active_leagues()
leagues = load_leagues()


# 5. Define Internal Page Functions
def team_overview():
    st.title("Performance Dashboard")
    # Now we can safely access session state because the sidebar has run
    if 'team' in st.session_state:
        st.write(f"Current Team Selection: **{st.session_state.team}**")

# 6. Define Navigation
pg_overview = st.Page('team_stats.py', title="Team Overview", icon="🏠")
pg_stats = st.Page("player_stats.py", title="Player Stats", icon="🏃")
#pg_team = st.Page("team_stats.py", title="Team Stats")
#pg_game_stats = st.Page("game_stats.py", title="Game Stats")
#pg_calendar = st.Page("cal_view.py", title="Calendar", icon="📅")

# 7. Run Navigation
pg = st.navigation([pg_overview, pg_stats])

# 2. Define which pages should NOT have the sidebar
# Use the exact string titles you defined above
PAGES_WITHOUT_SIDEBAR = [pg_stats] 


# 3. Conditionally Render the Sidebar
# 'pg.title' gives us the title of the currently active page
if pg not in PAGES_WITHOUT_SIDEBAR:
    with st.sidebar:
        st.header("Global Filters")
        
        # A. Get unique leagues for the dropdown
        # (Assuming 'league_name' is available and better for UI than ID)
        
        league_options = leagues['league_name'].sort_values().unique()
        league_id_map = dict(zip(leagues['league_name'], leagues['league_id']))

        # B. Create the League Selectbox
        # We assign the result to a variable 'selected_league_val' to use it immediately
        selected_league_val = st.selectbox(
            "Select League",
            options=league_options,
            key='league' 
        )
        
        # C. Filter Data based on the League Selection
        # This now works safely because 'selected_league_val' is defined right above
        df_filtered = al[al['league_id'] == league_id_map[selected_league_val]]
        
        # D. Get unique teams for the second dropdown
        team_options = df_filtered['team_name'].sort_values().unique()
        
        # E. Create the Team Selectbox
        st.selectbox(
            "Select Team",
            options=team_options,
            key='team'
        )
pg.run()