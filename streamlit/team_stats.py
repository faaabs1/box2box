import streamlit as st
import pandas as pd
import altair as alt
from db_conn.database import DatabaseClient
from data_entry.repository import FootballRepository
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



team_goals = load_data()

team_goals['home_goals'] = team_goals['home_goals'].fillna(0)
team_goals['away_goals'] = team_goals['away_goals'].fillna(0)
team_goals['total_goals'] = team_goals['total_goals'].fillna(0)
team_goals['home_goals_conceded'] = team_goals['home_goals_conceded'].fillna(0)
team_goals['away_goals_conceded'] = team_goals['away_goals_conceded'].fillna(0)


# team_goals['avg_goals_game'] = team_goals.apply(lambda x: x['total_goals'] / x['total_games'] if x['total_games'] > 0 else 0, axis=1)
# team_goals['avg_goals_home'] = team_goals.apply(lambda x: x['home_goals'] / x['home_games'] if x['home_games'] > 0 else 0, axis=1)
# team_goals['avg_goals_away'] = team_goals.apply(lambda x: x['away_goals'] / x['away_games'] if x['away_games'] > 0 else 0, axis=1)
# team_goals['avg_goals_conceded'] = team_goals.apply(lambda x: x['total_goals_conceded'] / x['total_games'] if x['total_games'] > 0 else 0, axis=1)

league_avg_metrics = ['total_goals','home_goals','away_goals']
for metric in league_avg_metrics:
    team_goals[f'league_avg_{metric}'] = league_avg(team_goals, metric)


goals_league_conceded_avg = team_goals['total_goals_conceded'].mean()

goals_league_75th = percentile(team_goals['total_goals'],75)
goals_league_75th_home = percentile(team_goals['home_goals'],75)
goals_league_75th_away = percentile(team_goals['away_goals'],75)
goals_league_conceded_75th = percentile(team_goals['total_goals_conceded'],75)



# # 5. Sidebar Filters
# st.sidebar.header("Filters")

# selected_teams = st.sidebar.selectbox(
#     "Select Teams",
#     options=team_goals['team_name'].sort_values().unique()
# )

# Filter the dataframe
filtered_df = team_goals[team_goals['team_name'] == st.session_state.team]



league_avg_goals = filtered_df['total_goals'].sum()-goals_league_avg
league_avg_goals = league_avg_goals.round(2)

league_avg_goals_home = filtered_df['home_goals'].sum()-goals_league_avg_home
league_avg_goals_home = league_avg_goals_home.round(2)

league_avg_goals_away = filtered_df['away_goals'].sum()-goals_league_avg_away
league_avg_goals_away = league_avg_goals_away.round(2)

league_75th_goals = (filtered_df['total_goals'].sum()-goals_league_75th)

league_75th_goals_home = (filtered_df['home_goals'].sum()-goals_league_75th_home)

league_75th_goals_away = (filtered_df['away_goals'].sum()-goals_league_75th_away)

league_75th_goals_conceded = (filtered_df['total_goals_conceded'].sum()-goals_league_conceded_75th)



# 6. Main Dashboard
st.title("⚽ Box2Box Analytics")
st.markdown(f"Performance metrics for **{st.session_state.team}**")

col1, col2= st.columns(2)
col1.metric("Total Goals scored",int(filtered_df['total_goals']),f"{league_75th_goals} vs. 75th percentile",border=True)
col2.metric("Total Goals conceded", int(filtered_df['total_goals_conceded']),f"{league_75th_goals_conceded} vs. 75th percentile",border=True)
#col3.metric("Away Goals", int(filtered_df['away_goals']),f"{league_75th_goals_away} vs. 75th percentile",border=True)



# col1, col2, col3 = st.columns(3)
# col1.metric("Avg Goals/Game", f"{filtered_df['avg_goals_game'].mean():.2f}")
# col2.metric("Avg Home Goals", f"{filtered_df['avg_goals_home'].mean():.2f}")
# col3.metric("Avg Away Goals", f"{filtered_df['avg_goals_away'].mean():.2f}")

st.divider()

st.subheader("Data Table")
st.dataframe(team_goals)