""" A library of functions to apply supported expressions to a polars DataFrane or
    LazyFrame for evaluation at the GUI level.
"""
from datetime import date
from typing import Any, Tuple
import polars as pl

class PolarsView():
    """ A class to alias the successive addition of predictable filter, group_by, and
        agg statements to a pl.DataFrame for a single return when expression building
        is complete.
    """

    def __init__(self, df):
        self._data_frame = df
        self._filter_expressions = []
        self._group_by_column = None
        self._group_by_step = None
        self._agg_expressions = []

    def extract_data_spread(self, col_name: str) -> Tuple[Any, Any]:
        """ Extract the first and last value in a sortable series from the tracked
            pl.DataFrame, without filtering applied.

            Arguments:
            col_name -- the name of the column to be examined
        """

        data_series = self._data_frame.get_column(col_name)
        return (data_series.min(), data_series.max())

    def extract_data_total(self, col_name: str) -> int:
        """ Extract the sum of all entries in the specified numeric column of the
            tracked pl.DataFrame.

            Arguments:
            col_name -- the name of the column to extract
        """
        return self._data_frame.get_column(col_name).sum()

    def apply_dynamic_date_grouping(self, selection_rank: str, col_name: str='date') -> str:
        """ Sets the dynamic grouping values to predetermined values according to the
            scale string specified by the user.

            Arguments:
            selection_rank -- a string of values 'Year', 'Month', or 'Day'
            col_name       -- (optional) the name of the column to apply grouping against (default 'date')
        """

        self.validate_column_name(col_name)

        match selection_rank:
            case 'Year':
                self._group_by_column = col_name
                self._group_by_step = '1y'
                disp_fmt = 'YYYY'
            case 'Month':
                self._group_by_column = col_name
                self._group_by_step = '1m'
                disp_fmt = 'YYYY MMMM'
            case 'Day':
                self._group_by_column = col_name
                self._group_by_step = '1d'
                disp_fmt = 'YYYY MMM DD'
            case _:
                self._group_by_column = None
                self._group_by_step = None
                raise ValueError(f"Unable to apply grouping based on input value '{selection_rank}'!")

        return disp_fmt

    def apply_filter_ge(self, col_name: str, min_value: Any):
        """ Appends a 'greater than or equal to' filter conditions to the user-specified
            column, to be assessed when the statement is collected.

            Arguments:
            col_name   -- the name of the column to attach the filter against
            min_value  -- the value to filter against in the expression
        """

        self.validate_column_name(col_name)

        self._filter_expressions.append(
            pl.col(col_name).ge(min_value)
        )

    def apply_filter_le(self, col_name: str, max_value: Any):
        """ Appends a 'less than or equal to' filter conditions to the user-specified
            column, to be assessed when the statement is collected.

            Arguments:
            col_name   -- the name of the column to attach the filter against
            max_value  -- the value to filter against in the expression
        """

        self.validate_column_name(col_name)

        self._filter_expressions.append(
            pl.col(col_name).le(max_value)
        )

    def apply_aggregation_rule(self, col_name: str, col_func: Any) -> str:
        """ Applies a summarisation rule to a column and aliases the new column name.
            Summarised column name is returned to the calling function for custom formating
            or any post-hoc manipulation.

            Arguments:
            col_name -- the name of the column to be summarised
            col_func -- a supported polars aggregation function, such as pl.sum, pl.min.
        """

        self.validate_column_name(col_name)

        col_label, agg_expr = PolarsView.build_summary_expr(col_name, col_func)
        self._agg_expressions.append(agg_expr)

        return col_label

    def apply_series_rule(self, col_name: str, dtype: Any=float) -> str:
        """ Applies a polars `implode()` function to a numeric column, creating a sequence of values
            in the resulting column. This allows for a temporal view of the summarised values rather
            that a summarisation such as provided by the `apply_aggregation_rule()` function.

            Arguments:
            col_name -- the name of the column to be summarised
            dtype    -- (optional) the return_dtype value for the transformation (default = float)
        """

        self.validate_column_name(col_name)

        col_label = f"{col_name}_implode"
        sequence_expr = (
            pl
            .col(col_name)
            .cast(dtype)
            .implode()
            .alias(col_label)
        )

        self._agg_expressions.append(sequence_expr)

        return col_label

    def resolve_view(self) -> pl.DataFrame:
        """ Execute the filtering and grouping conditions attached to the tracked data frame
            and return a summarised view of the data contained.

            Arguments:
            None
        """

        lf = self._data_frame.lazy()

        if self._filter_expressions:
            lf = lf.filter(self._filter_expressions)

        # Flow control for if these aren't set
        if self._group_by_column and self._group_by_step:
            lf = (
                lf
                .group_by_dynamic(self._group_by_column, every=self._group_by_step)
                .agg(self._agg_expressions)
            )

        return lf.collect()

    def validate_column_name(self, col_name: str):
        """ Confirm that the specified column exists in the tracked pl.DataFrame.

            Arguments:
            col_name -- the name of the column to be tested
        """

        if not col_name in self._data_frame.columns:
            raise ValueError(f"Column name '{col_name}' not found in data view!")

#region Static functions

    @staticmethod
    def build_summary_expr(col_name: str, col_func: Any) -> Tuple[str, Any]:
        """ Creates a column expression with computed alias, for when the PolarsView
            is resolved. Equivalent of statement:

            ```python
            # Function call
            PolarsView.build_summary_expr('value', pl.sum)

            # Is equivalent to
            pl.col('value').sum().alias('sum_value')
            ```

            Returns the dynamically produced label for the summary column

            Arguments:
            col_name -- the name of the column to be summarised
            col_func -- a supported polars aggregation function, such as pl.sum, pl.min.
        """

        col_label = f"{col_func.__name__}_{col_name}"
        expr = col_func(col_name).alias(col_label)

        return col_label, expr

#endregion