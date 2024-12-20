import unittest
from datetime import date

import polars as pl
from polars.testing import assert_frame_equal
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

from lib.sql_country import Country
from lib.sql_gdelt_record import GdeltRecord
from lib.sql_geotag import GeoTag
from lib.sql_mapping import BASE

DEFAULT_COUNTRY_1 = 'ABC'
DEFAULT_COUNTRY_2 = 'DEF'
DEFAULT_GEOTAG_1 = -1
DEFAULT_GEOTAG_2 = -1
DEFAULT_GEOTAG_3 = -1

class TestGdeltRecord(unittest.TestCase):
    """ Unit test class for the lib.GdeltRecord class """

    def setUp(self):

        self.engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
        BASE.metadata.create_all(self.engine)

        # Add two countries, for allowing FK links for new objects
        Country.create_record(self.engine, DEFAULT_COUNTRY_1, 'DEFAULT_COUNTRY_1')
        Country.create_record(self.engine, DEFAULT_COUNTRY_2, 'DEFAULT_COUNTRY_2')

        # Add three GeoTag records, for allowing FK links for new objects
        DEFAULT_GEOTAG_1 = GeoTag.create_new_record(self.engine, 1, 2.0, 3.0)
        DEFAULT_GEOTAG_1 = GeoTag.create_new_record(self.engine, 4, 5.0, 6.0)
        DEFAULT_GEOTAG_1 = GeoTag.create_new_record(self.engine, 7, 8.0, 9.0)

    def select_record(self, record_id) -> GdeltRecord:
        """ An internal helper method to extract a specified record without going through the
            GdeltRecord class.

            Arguments:
            record_id -- the GdeltRecord ID for the record to return
        """

        with Session(self.engine) as session:
            my_record = session.execute(
                select(GdeltRecord)
                .where(GdeltRecord.record_key == record_id)
            ).first()

        return my_record[0] if my_record else None

#region Overhead

    def test_init_required(self):
        """ Test the constructor for the GdeltRecord class when only the required fields
            are populated.
        """

        date_stamp = date.today()

        my_record = GdeltRecord(
            record_key=1,
            date=date_stamp,
            source_id=DEFAULT_COUNTRY_1,
            target_id=DEFAULT_COUNTRY_2,
            cameo_code=2,
            num_events=3,
            num_arts=4,
            quad_class=5.0,
        )

        self.assertEqual(1, my_record.record_key)
        self.assertEqual(date_stamp, my_record.date)
        self.assertEqual(DEFAULT_COUNTRY_1, my_record.source_id)
        self.assertEqual(DEFAULT_COUNTRY_2, my_record.target_id)
        self.assertEqual(2, my_record.cameo_code)
        self.assertEqual(3, my_record.num_events)
        self.assertEqual(4, my_record.num_arts)
        self.assertEqual(5.0, my_record.quad_class)
        self.assertIsNone(my_record.goldstein)
        self.assertIsNone(my_record.source_record)
        self.assertIsNone(my_record.target_record)
        self.assertIsNone(my_record.action_record)

    def test_init_optional(self):
        """ Test the constructor for the GdeltRecord class when the optional fields
            are populated. The required fields are not tested here, as they are already
            validated in the `test_init_required()` test.
        """

        date_stamp = date.today()

        my_record = GdeltRecord(
            record_key=1, date=date_stamp, source_id=DEFAULT_COUNTRY_1, target_id=DEFAULT_COUNTRY_2, cameo_code=2, num_events=3, num_arts=4, quad_class=5.0,
            goldstein=6.0,
            source_record_id=7,
            target_record_id=8,
            action_record_id=9,
        )

        self.assertEqual(6.0, my_record.goldstein)
        self.assertEqual(7, my_record.source_record_id)
        self.assertEqual(8, my_record.target_record_id)
        self.assertEqual(9, my_record.action_record_id)

    def test_eq(self):
        """ Test the equality operation for the GdeltRecord class. This is not exhaustively tested
            due to the number of variables to be tested.
        """

        date_stamp = date.today()

        first_record = GdeltRecord(
            date=date_stamp, source_id=DEFAULT_COUNTRY_1, target_id=DEFAULT_COUNTRY_2,
            cameo_code=2, num_events=3, num_arts=4, quad_class=5.0,
            source_record_id=6, target_record_id=7, action_record_id=8,
        )
        second_record = GdeltRecord(
            date=date_stamp, source_id=DEFAULT_COUNTRY_1, target_id=DEFAULT_COUNTRY_2,
            cameo_code=2, num_events=3, num_arts=4, quad_class=5.0,
            source_record_id=6, target_record_id=7, action_record_id=8,
        )

        third_record = GdeltRecord(
            date=date_stamp, source_id=DEFAULT_COUNTRY_1, target_id=DEFAULT_COUNTRY_2,
            cameo_code=3, num_events=3, num_arts=4, quad_class=5.0,
            source_record_id=6, target_record_id=7, action_record_id=8,
        )

        self.assertEqual(first_record, second_record)
        self.assertNotEqual(first_record, third_record)

    def test_repr(self):
        """ Test the to-string behaviour for the GdeltRecord class. """

        date_stamp = date.today()
        my_record = GdeltRecord(
            date=date_stamp, source_id=DEFAULT_COUNTRY_1, target_id=DEFAULT_COUNTRY_2,
            cameo_code=2, num_events=3, num_arts=4, quad_class=5.0,
        )

        # Representation of how SQLAlchemy will return the date string, which is not the same as a python
        # str(x) representation.
        date_string = f"datetime.date({date_stamp.year}, {date_stamp.month}, {date_stamp.day})"

        exp_message = (
            "GdeltRecord("
            f"date={date_string}, source_id='{my_record.source_id}', target_id='{my_record.target_id}', "
            f"cameo_code={my_record.cameo_code}, num_events={my_record.num_events}, num_arts={my_record.num_arts}, "
            f"quad_class={my_record.quad_class}, goldstein={my_record.goldstein}, "
            f"source_record_id={my_record.source_record_id}, target_record_id={my_record.target_record_id}, "
            f"action_record_id={my_record.action_record_id}"
            ")"
        )

        self.assertEqual(exp_message, str(my_record))

#endregion

#region Private functions

    def test_resolve_country_entry_country(self):
        """ Tests the behaviour of the `_resolve_country_entry()` function when the data is
            of type Country.
        """

        my_country = Country(code='ABC', name='alphabet')

        # Testing that the same object is returned
        obs_country = GdeltRecord._resolve_country_entry(self.engine, my_country)
        self.assertIs(my_country, obs_country)

    def test_resolve_country_entry_code(self):
        """ Tests the behaviour of the `_resolve_country_entry()` function when the data is
            the country code.
        """

        my_country = Country.select_by_id(self.engine, DEFAULT_COUNTRY_1)

        # Test that the Country instances are equal, but not necessarily the same instance
        obs_country = GdeltRecord._resolve_country_entry(self.engine, my_country.code)
        self.assertEqual(my_country, obs_country)

#endregion

#region Create operations

    def test_create_record_country(self):
        """ Test the behaviour of the `create_record()` function for a successful insertion operation
            for all required parameters when using Country objects.
        """

        country_1 = Country.select_by_id(self.engine, DEFAULT_COUNTRY_1)
        country_2 = Country.select_by_id(self.engine, DEFAULT_COUNTRY_2)
        date_stamp = date.today()

        exp_record = GdeltRecord(
            date=date_stamp,
            source_id=DEFAULT_COUNTRY_1,
            target_id=DEFAULT_COUNTRY_2,
            cameo_code=1,
            num_events=2,
            num_arts=3,
            quad_class=4,
        )

        GdeltRecord.create_record(self.engine, date_stamp, country_1, country_2, 1, 2, 3, 4)

        obs_record = self.select_record(1)
        self.assertEqual(exp_record, obs_record)

    def test_create_record_country_id(self):
        """ Test the behaviour of the `create_record()` function for a successful insertion operation
            for all required parameters when using Country codes as the record.
        """

        date_stamp = date.today()
        exp_record = GdeltRecord(
            date=date_stamp,
            source_id=DEFAULT_COUNTRY_1,
            target_id=DEFAULT_COUNTRY_2,
            cameo_code=1,
            num_events=2,
            num_arts=3,
            quad_class=4,
        )

        GdeltRecord.create_record(self.engine, date_stamp, DEFAULT_COUNTRY_1, DEFAULT_COUNTRY_2, 1, 2, 3, 4)

        obs_record = self.select_record(1)
        self.assertEqual(exp_record, obs_record)

    def test_create_record_optional(self):
        """ Test the behaviour of the `create_record()` function when passing optional parameters. Test
            the additional individually, assuming that they are additive when called in combination.
        """

        date_stamp = date.today()
        base_values = {
            'date': date_stamp,
            'source_id': DEFAULT_COUNTRY_1, 'target_id': DEFAULT_COUNTRY_2,
            'cameo_code': 1, 'num_events': 2, 'num_arts': 3, 'quad_class': 4
        }

        optional_values = [
            ('goldstein', 9),
            ('source_record', GeoTag.select_by_id(self.engine, DEFAULT_GEOTAG_1)),
            ('target_record', GeoTag.select_by_id(self.engine, DEFAULT_GEOTAG_2)),
            ('action_record', GeoTag.select_by_id(self.engine, DEFAULT_GEOTAG_3)),
        ]

        for i, (key, value) in enumerate(optional_values):
            with self.subTest(key=key, value=value):

                #my_record = base_record(date_stamp, )
                record_params = base_values | {key: value}
                my_record = GdeltRecord(**record_params)

                GdeltRecord.create_record(
                    self.engine,
                    date_stamp, my_record.source_id, my_record.target_id,
                    my_record.cameo_code, my_record.num_events, my_record.num_arts, my_record.quad_class,
                    **{key: value}
                )

                obs_record = self.select_record(i+1)
                self.assertEqual(my_record, obs_record)

    @unittest.skip('TODO')
    def test_create_mass_records(self):
        pass

#endregion

#region Select operations
 
    @unittest.skip('TODO')
    def test_select_all(self):
        pass

#endregion
