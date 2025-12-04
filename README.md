# box2box
End to end project for football analytics

## Project description: 
Goal of the project: 
- from data entry to be able to see all stats regarding our team and other teams in the league
- features to be developed: 
    - data entry for shots with coordinates to be able to train a xG model
    - use xG data for performance overview of own team
    - calculate xPoints to see if real points match performance
    - goalkeeper specific: use shot location and goalkeeper position to calc advanced models


**Following data can be entered (for now) and is saved directly to supabase DB:** \
- Game metadata information 
- Goal information 
- Lineup information 
- Card information 

Database used: Supabase
Flow of data: 
1) data entry through main.py: moves data to supabase raw schema
2) dbt process: transforms data and moves it into supabase analytics schema
3) data is visualized in the streamlit app


