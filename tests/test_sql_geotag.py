import unittest
import polars as pl
from polars.testing import assert_frame_equal
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

from lib.sql_geotag import GeoTag
from lib.sql_mapping import BASE

class TestGeoTag(unittest.TestCase):
    """ Unit test class for the lib.GeoTag class """

    def setUp(self):

        self.engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
        BASE.metadata.create_all(self.engine)

    def insert_geotag(self, geo_type: int, geo_lat: float, geo_long: float) -> None:
        """ Create a new record in the database without relying on the Country class. Uses the core approach rather
            than ORM to help troubleshoot errors in the Country class.

            Arguments:
            geo_type -- an integer value representing the record type
            geo_lat  -- the latitide of the record
            geo_long -- the longitude of the record
        """

        with self.engine.connect() as conn:
            conn.execute(
                insert(GeoTag)
                .values(geo_type=geo_type, geo_lat=geo_lat, geo_long=geo_long)
            )
            conn.commit()

    def select_geotag(self, geotag_id) -> GeoTag:
        """ An internal helper method to extract a specified GeoTag record without going through the
            GeoTag class.

            Arguments:
            geotag_id -- the primary key for the record to return
        """

        with Session(self.engine) as session:
            my_geotag = session.execute(
                select(GeoTag)
                .where(GeoTag.geo_id == geotag_id)
            ).first()

        return my_geotag[0] if my_geotag else None

#region Overhead

    def test_init(self):
        """ Test the constructor for the GeoTag class. """

        my_tag = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.75)
        self.assertEqual(1, my_tag.geo_type)
        self.assertEqual(0.5, my_tag.geo_lat)
        self.assertEqual(0.75, my_tag.geo_long)

    def test_eq(self):
        """ Test the equality operation for the GeoTag class. """

        first_tag = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.75)
        second_tag = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.75)
        third_tag = GeoTag(geo_type=2, geo_lat=0.5, geo_long=0.75)
        fourth_tag = GeoTag(geo_type=1, geo_lat=0.6, geo_long=0.75)
        fifth_tag = GeoTag(geo_type=1, geo_lat=0.6, geo_long=0.8)

        self.assertEqual(first_tag, second_tag)
        self.assertNotEqual(first_tag, third_tag)
        self.assertNotEqual(first_tag, fourth_tag)
        self.assertNotEqual(first_tag, fifth_tag)

    def test_repr(self):
        """ Test the to-string behaviour for the GeoTag class. """

        my_tag = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.75)
        self.assertEqual("GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.75)", str(my_tag))

#endregion

#region Private functions

    def test_find_existing_record(self):
        """ Test the behaviour of the `_find_existing_record()` when there is a match in the database. """

        # Populate the test database
        self.insert_geotag(1, 0.5, 0.8)
        self.insert_geotag(2, 0.6, 0.9)
        self.insert_geotag(3, 0.7, 1.0)

        # Based on insertion order, expect this to be the second record
        obs_pk = GeoTag._find_existing_record(self.engine, 2, 0.6, 0.9)
        self.assertEqual(2, obs_pk)

    def test_find_existing_record_no_match(self):
        """ Test the behaviour of the `_find_existing_record()` when there is no match in the database. """

        self.insert_geotag(1, 0.5, 0.8)
        self.insert_geotag(2, 0.6, 0.9)
        self.insert_geotag(3, 0.7, 1.0)

        obs_result = GeoTag._find_existing_record(self.engine, 4, 0.6, 0.9)
        self.assertIsNone(obs_result)

    def test_find_existing_record_partial_match(self):
        """ Test the behaviour of the `_find_existing_record()` when there is only a match to records in
            the database.
        """

        self.insert_geotag(1, 0.5, 0.8)
        self.insert_geotag(2, 0.6, 0.9)
        self.insert_geotag(3, 0.7, 1.0)

        mismatched_values = [
            (3, 0.6, 0.9), # Type mismatch
            (2, 0.7, 0.9), # GeoLat mismatch
            (2, 0.6, 0.8), # GeoLong mismatch
        ]

        for (geo_type, geo_lat, geo_long) in mismatched_values:
            with self.subTest(geo_type=geo_type, geo_lat=geo_lat, geo_long=geo_long):

                obs_result = GeoTag._find_existing_record(self.engine, geo_type, geo_lat, geo_long)
                self.assertIsNone(obs_result)

#endregion

#region Create operations

    def test_create_new_record_insert(self):
        """ Test the success case for the `create_new_record()` function when an insert operation is performed. """

        exp_geotag = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.8)

        obs_pk = GeoTag.create_new_record(self.engine, 1, 0.5, 0.8)
        self.assertEqual(obs_pk, 1)

        obs_geotag = self.select_geotag(obs_pk)
        self.assertEqual(exp_geotag, obs_geotag)

    def test_create_new_record_exists(self):
        """ Test the success case for the `create_new_record()` function when an identical record was already in the
            database.
        """

        exp_geotag = GeoTag(geo_type=2, geo_lat=0.6, geo_long=0.9)

        self.insert_geotag(1, 0.5, 0.8)
        self.insert_geotag(2, 0.6, 0.9)
        self.insert_geotag(3, 0.7, 1.0)

        obs_pk = GeoTag.create_new_record(self.engine, 2, 0.6, 0.9)
        self.assertEqual(obs_pk, 2)

        obs_geotag = self.select_geotag(obs_pk)
        self.assertEqual(exp_geotag, obs_geotag)

    def test_create_new_record_none(self):
        """ Test the behaviour of the `create_new_record()` function when the values are none. """

        obs_result = GeoTag.create_new_record(self.engine, None, None, None)
        self.assertIsNone(obs_result)

    def test_create_mass_records(self):
        """ Test the behaviour of the `create_mass_records()` function for a bulk insert operation. """

        insert_values = [
            {'geo_type': 1, 'geo_lat': 0.5, 'geo_long': 0.8},
            {'geo_type': 2, 'geo_lat': 0.6, 'geo_long': 0.9},
            {'geo_type': 3, 'geo_lat': 0.7, 'geo_long': 1.0},
        ]

        GeoTag.create_mass_records(self.engine, insert_values)

        for i, values in enumerate(insert_values):

            exp_geotag = GeoTag(**values)
            obs_geotag = self.select_geotag(i+1)
            self.assertEqual(exp_geotag, obs_geotag)

#endregion

#region Select operations

    def test_select_all(self):
        """ Test the behaviour of the `select_all()` function when the database is populated.
        """

        exp_df = pl.DataFrame([
            pl.Series('Geo_ID', [1, 2, 3]),
            pl.Series('GeoType', [1, 2, 3]),
            pl.Series('GeoLat', [0.5, 0.6, 0.7]),
            pl.Series('GeoLong', [0.8, 0.9, 1.0]),
        ])

        self.insert_geotag(1, 0.5, 0.8)
        self.insert_geotag(2, 0.6, 0.9)
        self.insert_geotag(3, 0.7, 1.0)

        obs_df = GeoTag.select_all(self.engine)
        assert_frame_equal(exp_df, obs_df)

    def test_select_all_empty(self):
        """ Test the behaviour of the `select_all()` function when the database is empty.
        """

        obs_df = GeoTag.select_all(self.engine)
        self.assertTrue(obs_df.is_empty())

    def test_select_by_id(self):
        """ Test the behaviour of the `select_by_id()` function when there is a match in the
            database.
        """

        exp_record = GeoTag(geo_type=1, geo_lat=0.5, geo_long=0.9)

        # First value in database, so ID = 1
        self.insert_geotag(1, 0.5, 0.9)
        obs_record = GeoTag.select_by_id(self.engine, 1)

        self.assertEqual(exp_record, obs_record)

    def test_select_by_id_empty(self):
        """ Test the behaviour of the `select_by_id()` function when there is no match in the
            database.
        """

        obs_record = GeoTag.select_by_id(self.engine, 1)
        self.assertIsNone(obs_record)

#endregion
