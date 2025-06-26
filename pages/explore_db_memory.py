import streamlit as st
from pages.ui_elements import init_session, render_sidebar

# Initialise the session if required
init_session(st.session_state)
render_sidebar()

st.markdown('# TO DO!')
