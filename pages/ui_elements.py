import streamlit as st
from lib.polars_view import PolarsView
from lib.sql_interface import DataInterface

def init_session(session_state: 'st.session_state', db_path: str='data/example.db'):
    """ Test the current session and add keys as required, initialising to expected
        default values.

        Arguments:
        session_state -- the current st.session_state object for the streamlit instance.
        db_path       -- (optional) the path to the GDELT data for viewing
    """

    # Assume that if one key is missing, all will be.
    if not 'db_connection' in session_state:
        session_state['db_connection'] = DataInterface.open_connection(db_path)

        # Initialise values for the table exploration view
        session_state['explore_table'] = None
        session_state['min_date'] = None
        session_state['max_date'] = None
        session_state['num_events'] = None

#region Reusable display elements

def render_sidebar():
    """ Draw the sidebar for the dashboard, and customise the naming of each page.

        Arguments:
        None
    """

    with st.sidebar:
        st.markdown('---')
        st.markdown('## Select an activity')
        st.page_link('pages/explore_country.py', label='Country exploration (table)')
        st.page_link('pages/visualise_longitudinal.py', label='Country exploration (graphical)')
        st.page_link('pages/visualise_spatial.py', label='Map projection of data')

def render_filter_panel(session_state: st.session_state, polars_view: PolarsView) -> None:
    """ Draw an expandable control with filter toggles to apply to a user-specified PolarsView
        representation of the data of interest. This function modifies the PolarsView directly
        so no value is returned.

        Arguments:
        session_state -- the current st.session_state object for the streamlit instance.
        polars_view   -- a PolarsView object representing a set of data extracted from the SQL database
    """

    with st.expander('Apply data filters!'):

        st.markdown('**Date range**')
        col_start_date, col_end_date = st.columns(2)

        with col_start_date:
            filt_start = st.date_input(
                'Start date',
                min_value=session_state.min_date,
                max_value=session_state.max_date,
                value=session_state.min_date
            )
            polars_view.apply_filter_ge('date', filt_start)

        with col_end_date:
            filt_end = st.date_input(
                'End date',
                min_value=session_state.min_date,
                max_value=session_state.max_date,
                value=session_state.max_date
            )
            polars_view.apply_filter_le('date', filt_end)

        st.divider()

        st.markdown('**Value ranges**')

        max_events = st.slider(
            'Constrain the maximum event number',
            max_value=session_state.max_events,
            value=session_state.max_events,
        )
        polars_view.apply_filter_le('num_events', max_events)

        # This is a bit of a cheat, as the score is bounded between these.
        min_goldstein, max_goldstein = st.select_slider(
            'Constrain the Goldstein scores to report',
            options=[i for i in range(-10, 11)],
            value=(-10, 10)
        )
        polars_view.apply_filter_ge('goldstein', min_goldstein)
        polars_view.apply_filter_le('goldstein', max_goldstein)

    return

#endregion

#region Table formatting

def format_column_progress(title: str, max_value: int, width='medium') -> st.column_config.ProgressColumn:
    """ Apply a predictable layout pattern for a ProgressBar column in a data frame.

        Arguments:
        title     -- the display name for the column to be formatted
        max_value -- the maximum value to use for the progress bar
        width     -- (optional) change the default column width value
    """

    # NEED TO REDEFINE THE MAX VALUE HERE

    return st.column_config.ProgressColumn(title, width=width, max_value=max_value, format='compact')

def format_column_areachart(title, width='small', y_min=-10, y_max=10) -> st.column_config.AreaChartColumn:
    """ Apply a predictable layout pattern for an AreaChart column in a data frame.

        Arguments:
        title -- the display name for the column to be formatted
        width -- (optional) change the default column width value
        y_min -- (optional) change the default minimum value for the chart
        y_max -- (optional) change the default maximum value for the chart
    """

    return st.column_config.AreaChartColumn(title, width=width, y_min=y_min, y_max=y_max)

#endregion
