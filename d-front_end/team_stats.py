import streamlit as st
import pandas as pd
import altair as alt
from a_db_conn.database import DatabaseClient
from b_data_entry_program.repository import FootballRepository
from numpy import percentile

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

def league_avg(df, column):
    return df[column].mean()

# 1. Load Data
team_goals = load_data()
#team_goals = team_goals[team_goals['league_id'] == st.session_state.league]
# 1. Identify all numeric columns (handles float32, int32, etc. automatically)


# 2. Fill all NaN values with 0
numeric_cols = team_goals.select_dtypes(include=['number']).columns

team_goals[numeric_cols] = team_goals[numeric_cols].fillna(0)

# 3. Calculate League averages and 75th percentiles

league_avg_metrics = ['total_goals','home_goals','away_goals','avg_goals_game','avg_goals_conceded']
for metric in league_avg_metrics:
    team_goals[f'league_avg_{metric}'] = league_avg(team_goals, metric)

goals_league_conceded_avg = team_goals['total_goals_conceded'].mean()

goals_league_75th = percentile(team_goals['total_goals'],75)
goals_league_75th_home = percentile(team_goals['home_goals'],75)
goals_league_75th_away = percentile(team_goals['away_goals'],75)
goals_league_conceded_75th = percentile(team_goals['total_goals_conceded'],75)
avg_goals_league_75th = percentile(team_goals['avg_goals_game'],75)
avg_goals_conceded_league_75th = percentile(team_goals['avg_goals_conceded'],75)    



# # 5. Sidebar Filters
# st.sidebar.header("Filters")

# selected_teams = st.sidebar.selectbox(
#     "Select Teams",
#     options=team_goals['team_name'].sort_values().unique()
# )
#points_per_game = team_goals['total_points'] / team_goals['total_games']
# Filter the dataframe
filtered_df = team_goals[team_goals['team_name'] == st.session_state.team]

filtered_df['points_per_game'] = team_goals['total_points'] / team_goals['total_games']


league_avg_goals = filtered_df['total_goals'].sum()-team_goals['league_avg_total_goals']
league_avg_goals = league_avg_goals.round(2)

league_avg_goals_home = filtered_df['home_goals'].sum()-team_goals['league_avg_home_goals']
league_avg_goals_home = league_avg_goals_home.round(2)

league_avg_goals_away = filtered_df['away_goals'].sum()-team_goals['league_avg_away_goals']
league_avg_goals_away = league_avg_goals_away.round(2)

league_75th_goals = (filtered_df['total_goals'].sum()-goals_league_75th)

league_75th_goals_home = (filtered_df['home_goals'].sum()-goals_league_75th_home)
league_75th_goals_away = (filtered_df['away_goals'].sum()-goals_league_75th_away)
league_75th_goals_conceded = (filtered_df['total_goals_conceded'].sum()-goals_league_conceded_75th)


league_75th_avg_goals_conceded = (filtered_df['avg_goals_conceded'].sum()-avg_goals_conceded_league_75th).round(2)
league_75th_avg_goals = (filtered_df['avg_goals_game'].sum()-avg_goals_league_75th).round(2)



# 6. Main Dashboard
st.title("⚽ Box2Box Analytics")
st.markdown(f"Performance metrics for **{st.session_state.team}**")

col1, col2, col3 = st.columns(3)
col1.metric("Points/Game", round(filtered_df['points_per_game'],2),border=True,height='stretch',width='stretch')
col2.metric("Total Goals scored",int(filtered_df['total_goals']),f"{league_75th_goals} vs. 75th percentile",border=True,height='stretch',width='stretch')
col3.metric("Goals/Game", round(filtered_df['avg_goals_game'],2),f"{league_75th_avg_goals} vs. 75th percentile",border=True,height='stretch',width='stretch')

col1, col2, col3 = st.columns(3)
col1.metric("Total Goals conceded", int(filtered_df['total_goals_conceded']),f"{league_75th_goals_conceded} vs. 75th percentile",border=True,delta_color="inverse")
col2.metric("Goals conceded/Game", round(filtered_df['avg_goals_conceded'],2),f"{league_75th_avg_goals_conceded } vs. 75th percentile",border=True,delta_color="inverse")
col3.metric("Goals conceded/Game", round(filtered_df['avg_goals_conceded'],2),f"{league_75th_avg_goals_conceded } vs. 75th percentile",border=True,delta_color="inverse")

col1, col2, col3 = st.columns(3)
col1.metric("Total Goals conceded", int(filtered_df['total_goals_conceded']),f"{league_75th_goals_conceded} vs. 75th percentile",border=True,delta_color="inverse")
col2.metric("Goals conceded/Game", round(filtered_df['avg_goals_conceded'],2),f"{league_75th_avg_goals_conceded } vs. 75th percentile",border=True,delta_color="inverse")
col3.metric("Goals conceded/Game", round(filtered_df['avg_goals_conceded'],2),f"{league_75th_avg_goals_conceded } vs. 75th percentile",border=True,delta_color="inverse")