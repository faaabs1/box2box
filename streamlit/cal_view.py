import streamlit as st
from streamlit_calendar import calendar

st.title("Streamlit Calendar View")

# 1. Define your events
# These are just dictionaries with 'title', 'start', and optionally 'end' or 'color'
events = [
    {
        "title": "Project Deadline",
        "color": "#FF6C6C",
        "start": "2025-12-25",
        "end": "2025-12-27",
    },
    {
        "title": "Team Meeting",
        "color": "#3D9DF3",
        "start": "2025-12-28T10:30:00",
        "end": "2025-12-28T12:30:00",
    },
]

# 2. Configure the calendar display options
calendar_options = {
    "editable": True, # Allow drag and drop
    "navLinks": True, # Allow clicking day numbers to switch views
    "headerToolbar": {
        "left": "today prev,next",
        "center": "title",
        "right": "dayGridMonth,timeGridWeek,timeGridDay",
    },
    "initialView": "dayGridMonth",
}

# 3. Render the calendar
state = calendar(
    events=events,
    options=calendar_options,
    custom_css="""
    .fc-event-past {
        opacity: 0.8;
    }
    .fc-event-time {
        font-style: italic;
    }
    .fc-event-title {
        font-weight: 700;
    }
    .fc-toolbar-title {
        font-size: 2rem;
    }
    """,
    key='my_calendar', # Essential to track state
)

# 4. Handle Interactions (Clicks)
if state.get("eventClick"):
    st.write(f"You clicked: {state['eventClick']['event']['title']}")