import streamlit as st
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

def render_sidebar():
    """ Draw the sidebar for the dashboard, and customise the naming of each page.
    """

    with st.sidebar:
        st.markdown('---')
        st.markdown('## Select an activity')
        st.page_link('pages/explore_db_disk.py', label='Table exploration')
        st.page_link('pages/explore_db_memory.py', label='Table exploration (in-memory)')

def format_column_progress(title: str, max_value: int, width='medium') -> st.column_config.ProgressColumn:
    """ Apply a predictable layout pattern for a ProgressBar column in a data frame.

        Arguments:
        title     -- the display name for the column to be formatted
        max_value -- the maximum value to use for the progress bar
        width     -- (optional) change the default column width value
    """

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
