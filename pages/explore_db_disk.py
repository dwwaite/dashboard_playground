import polars as pl
import streamlit as st
from lib.sql_country import Country
from lib.sql_gdelt_record import GdeltRecord

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
    df = GdeltRecord.select_by_country(st.session_state.db_connection, source_country, target_country=target_country)

    st.session_state.min_date = df.get_column('date').min()
    st.session_state.max_date = df.get_column('date').max()
    st.session_state.max_events = df.get_column('num_events').sum()
    st.session_state.explore_table = df

if not st.session_state.explore_table is None:

    with st.expander('Filter by dates'):
        col_start_date, col_end_date = st.columns(2)

        min_date = st.session_state.min_date
        max_date = st.session_state.max_date

        with col_start_date:
            filt_start = st.date_input('Start date', min_value=min_date, max_value=max_date, value=min_date)

        with col_end_date:
            filt_end = st.date_input('End date', min_value=min_date, max_value=max_date, value=max_date)

    with st.expander('Aggregate data'):

        selection_rank = st.segmented_control('Level to aggregate', ['Year', 'Month', 'Day'])

        agg_map = {'Minimum': pl.min, 'Maximum': pl.max, 'Count': pl.count, 'Total': pl.sum}
        agg_options = st.pills('Summary options', options=agg_map.keys(), selection_mode='multi')

    # Begin filter and render routine
    # Create a pl.LazyFrame() which can be manipulated in a builder-like pattern before collection
    disp_lf = st.session_state.explore_table.lazy()

    if filt_start != min_date:
        disp_lf = disp_lf.filter(pl.col('date').ge(filt_start))

    if filt_end != max_date:
        disp_lf = disp_lf.filter(pl.col('date').le(filt_end))

    # If both a summary rank and aggregation options are selected, perform the grouping.
    # Otherwise, report as raw data
    if selection_rank and agg_options:

        match selection_rank:
            case 'Year':
                disp_lf = disp_lf.group_by_dynamic('date', every='1y')
                disp_fmt = 'YYYY'
            case 'Month':
                disp_lf = disp_lf.group_by_dynamic('date', every='1m')
                disp_fmt = 'YYYY MMMM'
            case 'Day':
                disp_lf = disp_lf.group_by_dynamic('date', every='1d')
                disp_fmt = 'YYYY MMM DD'  

        agg_exprs = []
        config_map = {'date': st.column_config.DateColumn("Event date", format=disp_fmt)}

        def build_summary_expr(col_name, col_func):
            col_label = f"{col_func.__name__}_{col_name}"
            expr = col_func(col_name).alias(col_label)
            return col_label, expr

        # Iterate for num_events, then Goldstein, to keep the fields organised by their underlying data
        for agg_option in agg_options:
            col_label, agg_expr = build_summary_expr('num_events', agg_map[agg_option])
            agg_exprs.append(agg_expr)
            config_map[col_label] = format_column_progress(f"Number of events ({agg_option})", st.session_state.max_events)

        for agg_option in agg_options:
            col_label, agg_expr = build_summary_expr('goldstein', agg_map[agg_option])
            agg_exprs.append(agg_expr)
            config_map[col_label] = format_column_areachart(f"Goldstein Scale ({agg_option})")

        disp_lf = disp_lf.agg(agg_exprs)

    else:
        disp_fmt = 'YYYY MMM DD'
        config_map = {
            'date': st.column_config.DateColumn("Event date", format=disp_fmt),
            'num_events': format_column_progress('Number of events', st.session_state.max_events),
            'goldstein': format_column_areachart('Goldstein Scale'),
        }

    st.data_editor(
        disp_lf.collect().sort('date'),
        column_config=config_map,
        hide_index=True,
    )
