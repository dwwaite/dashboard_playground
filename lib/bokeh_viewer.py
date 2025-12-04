""" A class to abstract the binding and creation of bokeh plots from a polars DataFrame object.

"""
from typing import List
import polars as pl
import bokeh
from bokeh.palettes import tol
from bokeh.layouts import layout
from bokeh.plotting import figure
from bokeh.models import BoxAnnotation, ColumnDataSource, HoverTool, Label, RangeTool

class BokehViewer:

    def __init__(self, data_df: pl.DataFrame, **kwargs):

        # Can be extended as needed
        self._height = kwargs.get('height', 700)
        self._roll_window = kwargs.get('window_size', 20)
        self._events_top_n = kwargs.get('top_n', 10)

        # Bind the data with any transformations needed
        goldstein_df = BokehViewer.transform_goldstein(data_df, self._roll_window)
        self._goldstein_source = ColumnDataSource(data=goldstein_df)

        event_df, event_stackers = BokehViewer.transform_events(data_df, self._events_top_n)
        self._event_source = ColumnDataSource(data=event_df)
        self._event_stackers = event_stackers

    def _create_canvas(
        self,
        title_string: str=None,
        x_label: str=None,
        y_label: str=None,
        x_axis_location: str='below',
        tools: List[str]=['pan', 'wheel_zoom', 'save', 'reset'],
        output_backend: str='canvas',
    ) -> bokeh.plotting.figure:
        """ Create a bokeh figure instance with standardised values for key formatting parameters. Can
            customise the more common ones via keyword arguments. If a parameter of interest is not
            specified in this function the `fig.update()` command can be called after the fact to
            configure as required.

            Arguments:
            title_string    -- (optional) the title for the plot figure (default: None)
            x_label         -- (optional) the x-axis label for the plot figure (default: None)
            y_label         -- (optional) the y-axis label for the plot figure (default: None)
            x_axis_location -- (optional) the location for the x-axis label (default: below)
            tools           -- (optional) the selection of tools to be attached to the figure (default: pan, wheel_zoom, save, reset)
            output_backend  -- (optional) the rendering backend for the figure (default: canvas)
        """

        return figure(
            title=title_string,
            height=self._height,
            x_axis_label=x_label,
            y_axis_label=y_label,
            x_axis_type='datetime',
            x_axis_location=x_axis_location,
            tools=tools,
            output_backend=output_backend,
        )

    def plot_goldstein(self, title_string: str, **kwargs) -> bokeh.plotting.figure:
        """ Render the Goldstein scale plot with a range tool for dynamically selecting a focus
            region. Accepts arbitrary kwargs for customising plot height and colouring.

            Arguments:
            title_string -- the main figure title
            kwargs
              range_height -- the height of the range finder plot (default: 200)
              line_color   -- the colour of the vbar and line trace (default: #8856a7)
              fill_color   -- the colour of the area fill (default: #8856a7)
        """

        range_height = kwargs.get('range_height', 200)

        line_color = kwargs.get('line_color', '#8856a7')
        fill_color = kwargs.get('fill_color', '#9ebcda')

        # Add the main trace and the area fill
        fig = self._create_canvas(title_string=title_string, x_axis_location='above', y_label='Goldstein scale', output_backend='webgl')
        fig.update(height=self._height - range_height)

        fig.vbar(x='date', top='goldstein', source=self._goldstein_source, color=line_color)

        # Add the range tool
        r_fig = self._create_canvas(title_string='Drag to select period of interest', x_label='Date', tools='')
        r_fig.update(
            y_range=fig.y_range,
            toolbar_location=None,
            height=range_height,
        )

        r_fig.add_tools(RangeTool(x_range=fig.x_range, start_gesture='pan'))
        r_fig.line(x='date', y='rolling_goldstein', source=self._goldstein_source, color=line_color)
        r_fig.varea(x='date', y1='y_base', y2='rolling_goldstein', source=self._goldstein_source, color=fill_color)

        return layout(children=[fig, r_fig])

    def plot_country_events(self, title_string: str, **kwargs) -> bokeh.plotting.figure:
        """ Render the events per country plot. Accepts arbitrary kwargs for customising
            plot height and colouring.

            Arguments:
            title_string -- the main figure title
            kwargs
              None yet supported
        """

        fig = self._create_canvas(title_string=title_string, y_label='Number of events')

        fig.varea_stack(
            self._event_stackers,
            x='date',
            source=self._event_source,
            color=tol['Iridescent'][len(self._event_stackers)],
        )

        return fig

    @staticmethod
    def transform_goldstein(df: pl.DataFrame, window_size: int) -> pl.DataFrame:
        """ Perform required data transformation to ready the raw data for the Goldstein plots

            Arguments:
            df          -- the raw data to be transformed
            window_size -- the window size for performing the rolling average calculation
        """

        return (
            df
            .with_columns(
                y_base=pl.lit(0),
                rolling_goldstein=pl.col('goldstein').rolling_mean(window_size=window_size),
            )
        )

    @staticmethod
    def transform_events(df: pl.DataFrame, top_n: int) -> pl.DataFrame:
        """ Perform required data transformation to ready the raw data for the events per country
            plots. Where the numbe of entries is greater then the specified maximum, only the top N
            are retained and all others are condensed to a single entry.

            Arguments:
            df    -- the raw data to be transformed
            top_n -- the maximum number of country values to display
        """

        lf = df.lazy()

        if df.get_column('target_id').unique().len() > top_n:
            top_hits = (
                df
                .group_by('target_id')
                .agg(pl.col('num_events').sum())
                .sort('num_events', descending=True)
                .head(top_n)
                .get_column('target_id')
            )
            lf = (
                lf
                .with_columns(
                    pl
                    .when(pl.col('target_id').is_in(top_hits))
                    .then('target_id')
                    .otherwise(pl.lit('Other'))
                )
            )
            hit_order = sorted(top_hits) + ['Other']

        else:
            hit_order = df.get_column('target_id').unique().sort()

        event_df = (
            lf
            .collect()
            .pivot(index='date', on='target_id', values='num_events', aggregate_function='sum')
            .select(hit_order + ['date'])
        )

        return event_df, hit_order