import polars as pl
import streamlit as st
from lib.sql_country import Country
from lib.sql_gdelt_record import GdeltRecord
from lib.polars_view import PolarsView

from pages.ui_elements import init_session, render_sidebar
from pages.ui_elements import format_column_areachart, format_column_progress

# Initialise the session if required
init_session(st.session_state)
render_sidebar()

st.markdown('# View records over time')

# Load the data for visualisation, adding a None padding value at the front to allow no selection
country_list = [None] + Country.select_all(st.session_state.db_connection)

col1, col2 = st.columns(2)

with col1:
    source_country = st.selectbox(
        'Source location',
        country_list,
        format_func=lambda c: c.name if c else '---',
        help='The location from which records originate (GDELT includes both countries and continents in this category).'
    )

if source_country:
    with col2:
        target_country = st.selectbox(
            'Target location',
            country_list,
            format_func=lambda c: c.name if c else '---',
            help='The location about which records are made (GDELT includes both countries and continents in this category).'
        )
else:
    target_country = None

if st.button('Import selection'):

    st.session_state.explore_table = GdeltRecord.select_by_country(
        st.session_state.db_connection,
        source_country,
        target_country=target_country
    )

if not st.session_state.explore_table is None:

    polars_view = PolarsView(st.session_state.explore_table)

    (st.session_state.min_date, st.session_state.max_date) = polars_view.extract_data_spread('date')
    st.session_state.max_events = polars_view.extract_data_total('num_events')

    with st.expander('Filter by dates'):
        col_start_date, col_end_date = st.columns(2)

        with col_start_date:
            filt_start = st.date_input(
                'Start date',
                min_value=st.session_state.min_date,
                max_value=st.session_state.max_date,
                value=st.session_state.min_date
            )

        with col_end_date:
            filt_end = st.date_input(
                'End date',
                min_value=st.session_state.min_date,
                max_value=st.session_state.max_date,
                value=st.session_state.max_date
            )

    with st.expander('Aggregate data'):

        selection_rank = st.segmented_control('Level to aggregate', ['Year', 'Month', 'Day'])

        event_map = {'Minimum': pl.min, 'Maximum': pl.max, 'Count': pl.count, 'Total': pl.sum}
        goldstein_map = {'Temporal variation': pl.implode}

        col1, col2 = st.columns(2)

        with col1:
            event_options = st.pills('Event options', options=event_map.keys(), selection_mode='multi')

        with col2:
            goldstein_options = st.pills('Golstein options', options=goldstein_map.keys(), selection_mode='multi')

    # Begin filter and render routine
    if filt_start != st.session_state.min_date:
        polars_view.apply_filter_ge('date', filt_start)

    if filt_end != st.session_state.max_date:
        polars_view.apply_filter_le('date', filt_end)

    # If both a summary rank and aggregation options are selected, perform the grouping otherwise report raw
    if selection_rank and event_options:

        disp_fmt = polars_view.apply_dynamic_date_grouping(selection_rank)
        config_map = {'date': st.column_config.DateColumn("Event date", format=disp_fmt)}
        agg_exprs = []

        # Iterate for num_events to keep the fields organised by their underlying data
        for event_option in event_options:
            col_label = polars_view.apply_aggregation_rule('num_events', event_map[event_option])
            config_map[col_label] = format_column_progress(f"Number of events ({event_option})", st.session_state.max_events)

        for goldstein_option in goldstein_options:
            col_label = polars_view.apply_series_rule('goldstein')
            config_map[col_label] = format_column_areachart(f"Goldstein Scale)")

        disp_lf = polars_view.resolve_view().lazy()

    else:
        config_map = {
            'date': st.column_config.DateColumn("Event date", format='YYYY MMM DD'),
            'num_events': format_column_progress('Number of events', st.session_state.max_events),
            'goldstein': format_column_areachart('Goldstein Scale'),
        }
        disp_lf = polars_view._data_frame.lazy()

    st.data_editor(
        disp_lf.collect().sort('date'),
        column_config=config_map,
        hide_index=True,
    )
