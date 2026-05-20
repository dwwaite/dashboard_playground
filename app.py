import streamlit as st

from pages.ui_elements import init_session, mode_dialog, render_sidebar

st.set_page_config(
    page_title='GDELT exploration',
    page_icon=':earth_asia:', # Alternatively - :world_map:
    layout='wide',
)

# On first load, determine whether we are viewing in debug mode or not, and set the session state accordingly
if not 'run_mode' in st.session_state:
    mode_dialog()

else:
    # Initialise the session if required
    init_session(st.session_state)
    render_sidebar()
