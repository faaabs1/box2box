import streamlit as st

from a_db_conn.database import DatabaseClient
#from b_data_entry_program.repository import FootballRepository
from d_front_end.data_loader import DataLoader

# 1. Page Config
st.set_page_config(page_title="Box2Box Analytics", page_icon="⚽", layout="wide")

# 2. Connection Setup
@st.cache_resource
def get_repo():
    try:
        db = DatabaseClient()
        return DataLoader(db.get_client())
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        return None

repo = get_repo()

if not repo:
    st.stop()

# 3. Load Data
@st.cache_data
def fetch_leagues():
    active_leagues = repo.fetch_leagues()
    return active_leagues
def fetch_seasons():
    seasons = repo.fetch_seasons()
    return seasons

league = fetch_leagues()
season = fetch_seasons()

def fetch_teams(league_id):
    teams = repo.fetch_teams(league_id)
    return teams


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
    
        league_options = league['league_name'].sort_values().unique()
        league_id_map = dict(zip(league['league_name'], league['league_id']))
        

        season_options = season['season_name'].sort_values().unique()
        season_id_map = dict(zip(season['season_name'], season['season_id']))

        # B. Create the League Selectbox
        # We assign the result to a variable 'selected_league_val' to use it immediately
        selected_league_val = st.selectbox(
            "Select League",
            options=league_options,
            key='league' 
        )
        # Store the ID in session state
        st.session_state['selected_league_id'] = league_id_map[selected_league_val]
        selected_season_val = st.selectbox(
            "Select Season",
            options=season_options,
            key='season' 
        )
        # Store the ID in session state
        st.session_state['selected_season_id'] = season_id_map[selected_season_val]
        # C. Filter Data based on the League Selection
        # This now works safely because 'selected_league_val' is defined right above
        df_filtered = fetch_teams(league_id_map[selected_league_val])
        
        # D. Get unique teams for the second dropdown
        team_options = df_filtered['team_name'].sort_values().unique()
        
        # E. Create the Team Selectbox
        st.selectbox(
            "Select Team",
            options=team_options,
            key='team'
        )
        # Store the ID in session state
        st.session_state['selected_team_id'] = df_filtered[df_filtered['team_name'] == st.session_state.team]['team_id'].iloc[0]
pg.run()