import streamlit as st
import pandas as pd
import altair as alt
from database import DatabaseClient
from repository import FootballRepository
from numpy import percentile

# Page Configuration
st.set_page_config(
    page_title="Player Performance Cards",
    page_icon="⚽",
    layout="wide"
)

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

# Custom CSS for minor styling adjustments
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        text-align: center;
    }
    .player-card-header {
        margin-bottom: 0px;
    }
</style>
""", unsafe_allow_html=True)

# Mock Data Generation
def get_mock_data():
    return [
        {"name": "Erling Haaland", "team": "Manchester City", "minutes": 2450, "goals": 29, "starts": 28, "subs": 1},
        {"name": "Mohamed Salah", "team": "Liverpool", "minutes": 2200, "goals": 18, "starts": 25, "subs": 2},
        {"name": "Bukayo Saka", "team": "Arsenal", "minutes": 2300, "goals": 14, "starts": 27, "subs": 0},
        {"name": "Kevin De Bruyne", "team": "Manchester City", "minutes": 1400, "goals": 6, "starts": 16, "subs": 4},
        {"name": "Son Heung-min", "team": "Tottenham", "minutes": 2100, "goals": 15, "starts": 24, "subs": 1},
        {"name": "Marcus Rashford", "team": "Man Utd", "minutes": 1850, "goals": 7, "starts": 20, "subs": 5},
    ]
# 3. Load Data
@st.cache_data
def load_data():
    player_stats = repo.fetch_player_stats()
    return player_stats

player_stats = load_data()
# Helper function to generate avatar URL
def get_avatar_url(name):
    # Using UI Avatars API for dynamic initials
    return f"https://ui-avatars.com/api/?name={name.replace(' ', '+')}&background=random&size=128&bold=true"

def main():
    st.title("⚽ Player Metrics")
    st.write("Overview of player statistics for the current season.")
    st.markdown("---")

    # Filter controls (Sidebar)
    st.sidebar.header("Filters")
    min_goals = st.sidebar.slider("Minimum Goals", 0, 30, 0)
    
    #players = get_mock_data()
    
    # Filtering logic
    #filtered_players = [p for p in player_stats if p['goals_total'] >= min_goals]

    # if not filtered_players:
    #     st.warning("No players match the selected criteria.")
    #     return

    # Loop through players in steps of 2 to create rows
    for i in range(0, len(player_stats), 2):
        cols = st.columns(2)
        
        # Iterate over the 2 columns for the current row
        for j in range(2):
            # Check if there is a player for this column
            if i + j < len(player_stats):
                player = player_stats.iloc[i + j]
                
                with cols[j]:
                    # Create a container for the card (Streamlit 1.30+ border feature)
                    with st.container(border=True):
                        
                        # Create two main columns: Left for Image, Right for Info/Metrics
                        col_left, col_right = st.columns([1, 3], gap="medium")
                        
                        with col_left:
                            st.image(get_avatar_url(player['player_name']), width=100)
                            st.caption(player['player_name'])

                        with col_right:
                            st.subheader(player['player_name'])
                            
                            # Create 4 columns for the specific metrics
                            m1, m2, m3, m4 = st.columns(4)
                            
                            with m1:
                                st.metric(label="Mins", value=player['minutes_total'])
                            with m2:
                                st.metric(label="Goals", value=player['goals_total'])
                            with m3:
                                st.metric(label="Starts", value=player['starts_total'])

if __name__ == "__main__":
    main()