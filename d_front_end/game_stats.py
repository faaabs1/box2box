import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from mplsoccer import VerticalPitch
from numpy import random

# --- (Your existing imports and path fix can stay here) ---

st.title("Game Statistics")

# Mock data for demonstration
if 'team' not in st.session_state:
    st.warning("Please select a team on the Home page.")
    st.stop()

selected_team = st.session_state.team

st.header(f"Shot Map: {selected_team}")

# 1. Create the Pitch
# pitch_type='statsbomb' is standard (120x80), but you can use 'opta', 'uefa', etc.
pitch = VerticalPitch(pitch_type='statsbomb',half=True)

# 2. Draw the pitch using Matplotlib
# figsize determines the size in the Streamlit app
fig, ax = pitch.draw(figsize=(10, 7))

n_shots = 30
shot_x = random.randint(90, 118, n_shots) # x locations: 90 to 120 (attacking zone)
shot_y = random.randint(20, 65, n_shots)   # y locations: 0 to 80 (full width)
shot_outcomes = random.choice(['Goal', 'Miss', 'Saved'], n_shots)

# 3. Plot Mock Data (Shots)
# Let's say we have 3 shots: (x, y) coordinates
# StatsBomb coords: x (0-120), y (0-80)
shots_df = pd.DataFrame({
    'x': shot_x,
    'y': shot_y,
    'outcome': shot_outcomes
})

# Plot the points on the axes ('ax') we just created
pitch.scatter(shots_df.x, shots_df.y, ax=ax, s=150, c='red', edgecolors='white', label='Shots')

# 4. Display in Streamlit
st.pyplot(fig)

# Optional: Display the data below
with st.expander("See Shot Data"):
    st.dataframe(shots_df)