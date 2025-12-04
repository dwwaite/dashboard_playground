import streamlit as st
from lib.bokeh_viewer import BokehViewer
from lib.polars_view import PolarsView
from lib.sql_gdelt_record import GdeltRecord
from lib.sql_country import Country
from pages.ui_elements import init_session, render_country_import, render_filter_panel, render_sidebar

from streamlit_bokeh import streamlit_bokeh

# Initialise the session if required
init_session(st.session_state)
render_sidebar()

st.markdown('# Trace metrics over time')

# Load the data for visualisation, adding a None padding value at the front to allow no selection
# Store the selections into the session, as they need to persist when figures as toggled
country_list = [None] + Country.select_all(st.session_state.db_connection)
source_country, _ = render_country_import(country_list, select_target=False)

if st.button('Import selection'):
    st.session_state['source_country'] = source_country
    st.session_state.explore_table = GdeltRecord.select_by_country(
        st.session_state.db_connection,
        source_country,
    )

if not st.session_state.explore_table is None:

    polars_view = PolarsView(st.session_state.explore_table)

    st.session_state.min_date, st.session_state.max_date = polars_view.extract_data_spread('date')
    _, st.session_state.max_events = polars_view.extract_data_spread('num_events')

    render_filter_panel(st.session_state, polars_view)

    tab1, tab2, tab3 = st.tabs(['Goldstein scale', 'Event count', 'Data snapshot'])

    source_name = st.session_state['source_country'].name
    disp_df = polars_view.resolve_view()
    bokeh_viewer = BokehViewer(disp_df)

    with tab1:
        gs_fig = bokeh_viewer.plot_goldstein(f"{source_name} data")
        streamlit_bokeh(gs_fig, use_container_width=True, theme='streamlit', key='bokeh_goldstein')

    with tab2:
        event_fig = bokeh_viewer.plot_country_events(f"{source_name} events")
        streamlit_bokeh(event_fig, use_container_width=True, theme='streamlit', key='bokeh_events')

    with tab3:
        st.data_editor(disp_df.head())
