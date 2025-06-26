import polars as pl
import streamlit as st

from pages.ui_elements import init_session, render_sidebar

st.set_page_config(page_title='GDELT exploration')

# Initialise the session if required
init_session(st.session_state)

render_sidebar()
