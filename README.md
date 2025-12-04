# box2box
End to end project for football analytics

## Project description: 
**Following Data can be entered and is saved directly to supabase DB:** \
- Game metadata information \
- Goal information \
- Lineup information \
- Card information \

Database used: Supabase
Flow of data: 
1) data entry through main.py: moves data to supabase raw schema
2) dbt process: transforms data and moves it into supabase analytics schema
3) data is visualized in the streamlit app



