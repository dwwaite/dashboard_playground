import streamlit as st
from lib.bokeh_viewer import BokehViewer
from lib.polars_view import PolarsView
from lib.sql_gdelt_record import GdeltRecord
from lib.sql_country import Country
from pages.ui_elements import init_session, render_country_import, render_filter_panel, render_sidebar

from streamlit_bokeh import streamlit_bokeh

@st.dialog('Customise the view!')
def colour_picker():
    col1, col2 = st.columns(2)

    with col1:
        line_color = st.color_picker('Trace colour', '#8856a7')
    
    with col2:
        fill_color = st.color_picker('Fill colour', '#9ebcda')

    if st.button('Re-draw the figure'):
        st.session_state.line_color = line_color
        st.session_state.fill_color = fill_color
        st.rerun()

def tab_goldstein(plot_title: str, bokeh_viewer: BokehViewer):
    st.badge('Customisation rendered using `streamlit` backend')

    if st.button('Customise the view!'):
        colour_picker()

    if st.session_state.line_color and st.session_state.fill_color:
        gs_fig = bokeh_viewer.plot_goldstein(plot_title,line_color=st.session_state.line_color, fill_color=st.session_state.fill_color)

    else:
        gs_fig = bokeh_viewer.plot_goldstein(plot_title)

    streamlit_bokeh(gs_fig, use_container_width=True, theme='streamlit', key='bokeh_goldstein')

# Initialise the session if required
init_session(st.session_state)
render_sidebar()

st.markdown('# Trace metrics over time')

# Load the data for visualisation, adding a None padding value at the front to allow no selection
# Store the selections into the session, as they need to persist when figures as toggled
country_list = [None] + Country.select_all(st.session_state.db_connection)
source_country, _ = render_country_import(country_list, select_target=False)

if st.button('Import selection'):
    st.session_state.source_country = source_country
    st.session_state.plot_table = GdeltRecord.select_by_country(
        st.session_state.db_connection,
        source_country,
    )

if not st.session_state.plot_table is None:

    polars_view = PolarsView(st.session_state.plot_table)

    st.session_state.min_date, st.session_state.max_date = polars_view.extract_data_spread('date')
    _, st.session_state.max_events = polars_view.extract_data_spread('num_events')

    render_filter_panel(st.session_state, polars_view)

    st.divider()
    tab1, tab2, tab3 = st.tabs(['Goldstein scale', 'Event count', 'Data snapshot'])

    disp_df = polars_view.resolve_view()
    bokeh_viewer = BokehViewer(disp_df)

    with tab1:
        tab_goldstein(f"{st.session_state.source_country.name} data", bokeh_viewer)

    with tab2:
        event_fig = bokeh_viewer.plot_country_events(f"{st.session_state.source_country.name} events")
        streamlit_bokeh(event_fig, use_container_width=True, theme='streamlit', key='bokeh_events')

    with tab3:
        st.data_editor(disp_df.head())
