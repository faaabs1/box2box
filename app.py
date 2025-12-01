import streamlit as st
import pandas as pd
import altair as alt
from database import DatabaseClient
from repository import FootballRepository

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

team_goals['home_goals'] = team_goals['home_goals'].fillna(0)
team_goals['away_goals'] = team_goals['away_goals'].fillna(0)
team_goals['total_goals'] = team_goals['total_goals'].fillna(0)
team_goals['avg_goals_game'] = team_goals.apply(lambda x: x['total_goals'] / x['total_games'] if x['total_games'] > 0 else 0, axis=1)
team_goals['avg_goals_home'] = team_goals.apply(lambda x: x['home_goals'] / x['home_games'] if x['home_games'] > 0 else 0, axis=1)
team_goals['avg_goals_away'] = team_goals.apply(lambda x: x['away_goals'] / x['away_games'] if x['away_games'] > 0 else 0, axis=1)







# 5. Sidebar Filters
st.sidebar.header("Filters")
selected_teams = st.sidebar.selectbox(
    "Select Teams",
    options=team_goals['team_name'].sort_values().unique()
)

# Filter the dataframe
filtered_df = team_goals[team_goals['team_name'] == selected_teams]

# 6. Main Dashboard
st.title("⚽ Box2Box Analytics")
st.markdown("Performance metrics for Home and Away games.")

col1, col2, col3 = st.columns(3)
col1.metric("Total Goals", int(filtered_df['total_goals']))
col2.metric("Home Goals", int(filtered_df['home_goals']))
col3.metric("Away Goals", int(filtered_df['away_goals']))

col1, col2, col3 = st.columns(3)
col1.metric("Avg Goals/Game", f"{filtered_df['avg_goals_game'].mean():.2f}")
col2.metric("Avg Home Goals", f"{filtered_df['avg_goals_home'].mean():.2f}")
col3.metric("Avg Away Goals", f"{filtered_df['avg_goals_away'].mean():.2f}")

st.divider()

st.subheader("Data Table")
st.dataframe(team_goals)