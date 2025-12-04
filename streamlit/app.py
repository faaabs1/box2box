#app.py
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) 
sys.path.append(parent_dir)

import streamlit as st
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository

# 1. Page Config
st.set_page_config(page_title="Box2Box Analytics", page_icon="⚽", layout="wide")

# 2. Connection Setup (Cached to prevent reconnecting on every click)
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
def load_data():
    team_goals_df = repo.fetch_team_goals()
    return team_goals_df

team_goals = load_data()


# 3. Define Internal Page Functions
def team_overview():
    st.title("Performance Dashboard")
    st.write(f"Current Team Selection: **{st.session_state.team}**")

team_selected = team_goals['team_name'].sort_values().unique()


# 4. Global Sidebar (Shared across all pages)
# Because this is in the main body, it runs every time, keeping the state sync'd
with st.sidebar:
    st.header("Global Filters")
    st.selectbox(
        "Select Team",
        options=team_selected,
        key='team'
    )

# 5. Define Navigation
# Page 1: The function defined above
pg_overview = st.Page(team_overview, title="Team Overview", icon="🏠")

# Page 2: The external file
# Ensure 'player_stats.py' is in the same folder as this script
pg_stats = st.Page("player_stats.py", title="Player Stats", icon="🏃")

pg_team = st.Page("team_stats.py",title="Team Stats")

pg_game_stats = st.Page("game_stats.py",title="Game Stats")
# 6. Run Navigation
pg = st.navigation([pg_overview, pg_stats,pg_team,pg_game_stats])
pg.run()