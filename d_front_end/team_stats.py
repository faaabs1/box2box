import streamlit as st
import pandas as pd
import altair as alt
from a_db_conn.database import DatabaseClient
from b_data_entry_program.repository import FootballRepository
from d_front_end.data_loader import DataLoader
from numpy import percentile

# 1. Page Config
st.set_page_config(page_title="Box2Box Analytics", page_icon="⚽", layout="wide")

# 2. Connection Setup (Cached to prevent reconnecting on every click)
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
def load_data():
    team_goals_df = repo.fetch_team_stats(st.session_state.selected_league_id, st.session_state.selected_season_id, st.session_state.selected_team_id)
    return team_goals_df

def league_avg(df, column):
    return df[column].mean()

# 1. Load Data
team_goals = load_data()
#team_goals = team_goals[team_goals['league_id'] == st.session_state.league]
# 1. Identify all numeric columns (handles float32, int32, etc. automatically)

st.info('Site is coming soon!', icon="ℹ️")