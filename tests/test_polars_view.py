import unittest
from datetime import date

import polars as pl
from polars.testing import assert_frame_equal
from lib.polars_view import PolarsView

class TestPolarsView(unittest.TestCase):
    """ Unit test class for the lib.PolarsView class """

    def create_dummy_df(self):
        """ Creates a mock pl.DataFrame with two columns for use in testing.
        
            |a (pl.Int64)|b (pl.Utf8)|
            |:---:|:---:|
            |1|a|
            |2|b|
            |3|c|
        """
        return (
            pl
            .DataFrame([
                pl.Series('a', [1, 2, 3]),
                pl.Series('b', ['a', 'b', 'c']),
            ])
        )

    def test_init(self):
        """ Tests the constructor for the PolarsView class.
        """

        input_df = self.create_dummy_df()

        polars_view = PolarsView(input_df)

        self.assertIsInstance(polars_view, PolarsView)
        self.assertListEqual(polars_view._filter_expressions, [])
        self.assertListEqual(polars_view._agg_expressions, [])
        self.assertIsNone(polars_view._group_by_column)
        self.assertIsNone(polars_view._group_by_step)

        assert_frame_equal(polars_view._data_frame, input_df)

#region Data extraction

    def test_extract_data_spread(self):
        """ Tests the behaviour of the PolarsView.extract_data_spread() function.
        """

        exp_min = date(1986, 11, 11)
        exp_mid = date(1987, 12, 12)
        exp_max = date(2000, 1, 1)

        polars_view = PolarsView(
            self
            .create_dummy_df()
            .with_columns(pl.Series('date', [exp_mid, exp_min, exp_max]))
            .cast({'date': pl.Date})
        )

        obs_min, obs_max = polars_view.extract_data_spread('date')
        self.assertEqual(exp_min, obs_min)
        self.assertEqual(exp_max, obs_max)

    def test_extract_data_total(self):
        """ Tests the behaviour of the PolarsView.extract_data_total() function.
        """

        input_values = [1, 2, 3]
        polars_view = PolarsView(
            self
            .create_dummy_df()
            .with_columns([pl.Series('events', input_values)])
        )

        exp_value = sum(input_values)
        obs_value = polars_view.extract_data_total('events')

        self.assertEqual(exp_value, obs_value)

#endregion

#region PolarsView.apply_dynamic_date_grouping()

    def test_apply_dynamic_date_grouping(self):
        """ Validate the behaviour of the PolarsView.apply_dynamic_date_grouping() function
            for all accepted into values.
        """

        polars_view = PolarsView(
            self
            .create_dummy_df()
            .with_columns(pl.Series('date', [7, 8, 9]))
        )

        input_steps = ['Year', 'Month', 'Day']
        exp_steps = ['1y', '1m', '1d']
        exp_formats = ['YYYY', 'YYYY MMMM', 'YYYY MMM DD']
        for input_step, exp_step, exp_format in zip(input_steps, exp_steps, exp_formats):

            with self.subTest(input_step=input_step, exp_step=exp_step, exp_format=exp_format):

                obs_format = polars_view.apply_dynamic_date_grouping(input_step, 'date')

                self.assertEqual('date', polars_view._group_by_column)
                self.assertEqual(exp_step, polars_view._group_by_step)

                self.assertEqual(exp_format, obs_format)

    def test_apply_dynamic_date_grouping_fail_column(self):
        """ Validate the behaviour of the PolarsView.apply_dynamic_date_grouping() function
            when the grouping column does not exist.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_dynamic_date_grouping(None, dud_column)

        self.assertEqual(exp_error,str(error_context.exception))
        self.assertIsNone(polars_view._group_by_column)
        self.assertIsNone(polars_view._group_by_step)

    def test_apply_dynamic_date_grouping_fail_selection(self):
        """ Validate the behaviour of the PolarsView.apply_dynamic_date_grouping() function
            when the selection rank is an invalid value.
        """

        exp_error = "Unable to apply grouping based on input value 'None'!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_dynamic_date_grouping(None, 'a')

        self.assertEqual(exp_error,str(error_context.exception))
        self.assertIsNone(polars_view._group_by_column)
        self.assertIsNone(polars_view._group_by_step)

#endregion

#region PolarsView.apply_filter_ge() and PolarsView.apply_filter_le()

    def test_apply_filter_ge(self):
        """ Tests the behaviour of the PolarsView.apply_filter_ge() function. This test and
            its function of interest do not evaluate the validity of the expression or that
            the subsequent filtering is meaningful, only that it matches an existing column.
        """

        polars_view = PolarsView(self.create_dummy_df())
        exp_exprs = []

        for (a, b) in zip(['a', 'b'], [1, 2]):
            polars_view.apply_filter_ge(a, b)
            exp_exprs.append(pl.col(a).ge(b))

        for exp_expr, obs_expr in zip(exp_exprs, polars_view._filter_expressions):
            self.assertTrue(exp_expr.meta.eq(obs_expr))

    def test_apply_filter_ge_fail(self):
        """ Tests the behaviour of the PolarsView.apply_filter_ge() function when the column
            to be filtered is not in the tracked dataframe.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_filter_ge(dud_column, -1)

        self.assertEqual(exp_error,str(error_context.exception))
        self.assertListEqual(polars_view._filter_expressions, [])


    def test_apply_filter_le(self):
        """ Tests the behaviour of the PolarsView.apply_filter_le() function. This test and
            its function of interest do not evaluate the validity of the expression or that
            the subsequent filtering is meaningful, only that it matches an existing column.
        """

        polars_view = PolarsView(self.create_dummy_df())
        exp_exprs = []

        for (a, b) in zip(['a', 'b'], [1, 2]):
            polars_view.apply_filter_le(a, b)
            exp_exprs.append(pl.col(a).le(b))

        for exp_expr, obs_expr in zip(exp_exprs, polars_view._filter_expressions):
            self.assertTrue(exp_expr.meta.eq(obs_expr))

    def test_apply_filter_le_fail(self):
        """ Tests the behaviour of the PolarsView.apply_filter_le() function when the column
            to be filtered is not in the tracked dataframe.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_filter_le(dud_column, -1)

        self.assertEqual(exp_error, str(error_context.exception))
        self.assertListEqual(polars_view._filter_expressions, [])

#endregion

#region PolarsView.validate_column_name()

    def test_validate_column_name(self):
        """ Tests the behaviour of the PolarsView.validate_column_name() function.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        # Test for extant column
        obs_result = polars_view.validate_column_name('a')
        self.assertIsNone(obs_result)

        # Test for missing column
        with self.assertRaises(ValueError) as error_context:
            polars_view.validate_column_name(dud_column)

        self.assertEqual(exp_error,str(error_context.exception))

#endregion

#region PolarsView.apply_aggregation_rule() and PolarsView.apply_series_rule()

    def test_apply_aggregation_rule(self):
        """ Tests the behaviour of the PolarsView.apply_aggregation_rule() function
            using a sequence of three expressions.
        """

        polars_view = PolarsView(self.create_dummy_df())

        exp_labels = [
            'median_a',
            'max_a',
            'count_b',
        ]

        exp_exprs = [
            pl.col('a').median().alias('median_a'),
            pl.col('a').max().alias('max_a'),
            pl.col('b').count().alias('count_b'),
        ]

        obs_labels = [
            polars_view.apply_aggregation_rule('a', pl.median),
            polars_view.apply_aggregation_rule('a', pl.max),
            polars_view.apply_aggregation_rule('b', pl.count),
        ]

        self.assertListEqual(exp_labels, obs_labels)

        for exp_expr, obs_expr in zip(exp_exprs, polars_view._agg_expressions):
            self.assertTrue(exp_expr.meta.eq(obs_expr))

    def test_apply_aggregation_rule_fail(self):
        """ Tests the behaviour of the PolarsView.apply_aggregation_rule() function
            when an invalid column name is used.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_aggregation_rule(dud_column, pl.max)

        self.assertEqual(exp_error, str(error_context.exception))
        self.assertListEqual(polars_view._agg_expressions, [])

    def test_apply_series_rule(self):
        """ Tests the behaviour of the PolarsView.apply_series_rule() function. Polars testing
            functionality does not allow direct comparison of `map_elements` expressions, so need
            to evaluate the expression to test the result.
        """

        polars_view = PolarsView(self.create_dummy_df().with_columns(grouper=pl.lit('c')))

        exp_label = 'a_implode'
        exp_value = [1.0, 2.0, 3.0]

        polars_view.apply_series_rule('a')
        obs_df = (
            polars_view
            ._data_frame
            .group_by('grouper')
            .agg(polars_view._agg_expressions)
        )

        self.assertListEqual(exp_value, obs_df.get_column(exp_label).first())

    def test_apply_series_rule_dtype(self):
        """ Tests the behaviour of the PolarsView.apply_series_rule() function when a non-default
            return_dtype value is provided.
        """

        polars_view = PolarsView(self.create_dummy_df().with_columns(grouper=pl.lit('c')))

        exp_label = 'a_implode'
        exp_value = [1, 2, 3]

        polars_view.apply_series_rule('a', dtype=int)
        obs_df = (
            polars_view
            ._data_frame
            .group_by('grouper')
            .agg(polars_view._agg_expressions)
        )

        self.assertListEqual(exp_value, obs_df.get_column(exp_label).first())

    def test_apply_series_rule_fail(self):
        """ Tests the behaviour of the PolarsView.apply_series_rule() function when an invalid
        column name is used.
        """

        dud_column = 'banana'
        exp_error = f"Column name '{dud_column}' not found in data view!"

        polars_view = PolarsView(self.create_dummy_df())

        with self.assertRaises(ValueError) as error_context:
            polars_view.apply_series_rule(dud_column)

        self.assertEqual(exp_error, str(error_context.exception))
        self.assertListEqual(polars_view._agg_expressions, [])

#endregion

#region PolarsView.resolve_view()

    def test_resolve_view_filter_only(self):
        """ Tests the behaviour of the PolarsView.resolve_view() function when only the filtering parameter
            has been applied.
        """

        polars_view = PolarsView(self.create_dummy_df())

        exp_df = polars_view._data_frame.filter(pl.col('a').ge(2))

        polars_view.apply_filter_ge('a', 2)
        obs_df = polars_view.resolve_view()

        assert_frame_equal(exp_df, obs_df)

    def test_resolve_view_agg_only(self):
        """ Tests the behaviour of the PolarsView.resolve_view() function when only the aggregation parameter
            has been applied.
        """

        polars_view = PolarsView(
            self
            .create_dummy_df()
            .with_columns(
                pl.Series('grouping', [date(2000, 1, 1), date(2000, 1, 1), date(2001, 1, 1)])
            )
        )

        exp_df = (
            polars_view
            ._data_frame
            .group_by('grouping')
            .agg(
                median_a=pl.col('a').median()
            )
        )

        polars_view.apply_aggregation_rule('a', pl.median)
        polars_view.apply_dynamic_date_grouping('Year', col_name='grouping')
        obs_df = polars_view.resolve_view()

        # Polars can resolve in either order, so explicitly sort data before assessing
        assert_frame_equal(
            exp_df.sort('grouping'),
            obs_df.sort('grouping')
        )

    def test_resolve_view_filter_agg(self):
        """ Tests the behaviour of the PolarsView.resolve_view() function when both the filtering and
            aggregation parameters have been applied.
        """

        polars_view = PolarsView(
            self
            .create_dummy_df()
            .with_columns(
                pl.Series('grouping', [date(2000, 1, 1), date(2000, 1, 1), date(2001, 1, 1)])
            )
        )

        exp_df = (
            polars_view
            ._data_frame
            .filter(pl.col('a').ge(2))
            .group_by('grouping')
            .agg(
                median_a=pl.col('a').median()
            )
        )

        polars_view.apply_filter_ge('a', 2)
        polars_view.apply_aggregation_rule('a', pl.median)
        polars_view.apply_dynamic_date_grouping('Year', col_name='grouping')
        obs_df = polars_view.resolve_view()

        # Polars can resolve in either order, so explicitly sort data before assessing
        assert_frame_equal(
            exp_df.sort('grouping'),
            obs_df.sort('grouping')
        )

    def test_resolve_view_none(self):
        """ Tests the behaviour of the PolarsView.resolve_view() function when no filtering or
            aggregation is applied.
        """

        polars_view = PolarsView(self.create_dummy_df())

        assert_frame_equal(
            polars_view._data_frame,
            polars_view.resolve_view(),
        )

#endregion

#region Static functions

    def test_build_summary_expr(self):
        """ Tests the behaviour of the static PolarsView.build_summary_expr() function.
        """

        exp_label = 'median_input'
        exp_expr = pl.col('input').median().alias(exp_label)

        obs_label, obs_expr = PolarsView.build_summary_expr('input', pl.median)

        self.assertEqual(exp_label, obs_label)
        self.assertTrue(exp_expr.meta.eq(obs_expr))

#endregion
