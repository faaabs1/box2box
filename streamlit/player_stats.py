import streamlit as st
import pandas as pd
import altair as alt
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository
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

# 3. Load Data
@st.cache_data
def load_data():
    player_stats = repo.fetch_player_stats()
    return player_stats

player_stats = load_data()
#player_stats = player_stats.sort_values(by='jersey_number', ascending=True)
# Helper function to generate avatar URL
# def get_avatar_url(name):
#     # Using UI Avatars API for dynamic initials
#     return f"https://ui-avatars.com/api/?name={name.replace(' ', '+')}&background=random&size=128&bold=true"

def get_avatar_url(name):
    # Using UI Avatars API for dynamic initials
    return f"https://ui-avatars.com/api/?name={name}&background=black&size=64&bold=true"

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
    for row in range(0, len(player_stats), 2):
        cols = st.columns(2)
        
        # Iterate over the 2 columns for the current row
        for col in range(2):
            # Check if there is a player for this column
            if row + col < len(player_stats):
                player = player_stats.iloc[row + col]
                
                with cols[col]:
                    # Create a container for the card (Streamlit 1.30+ border feature)
                    with st.container(border=True):
                        
                        # Create two main columns: Left for Image, Right for Info/Metrics
                        col_left, col_right = st.columns([1, 3], gap="medium")
                        
                        with col_left:
                            st.image(get_avatar_url(player['jersey_number']), width=100)
                            st.caption(player['position1'])

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