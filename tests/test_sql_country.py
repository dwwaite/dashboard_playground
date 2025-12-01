import unittest
import polars as pl
from polars.testing import assert_frame_equal
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

from lib.sql_country import Country
from lib.sql_mapping import BASE

class TestCountry(unittest.TestCase):
    """ Unit test class for the lib.Country class """

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
        BASE.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def insert_country(self, country_code, country_name) -> None:
        """ Create a new record in the database without relying on the Country class. Uses the core approach rather
            than ORM to help troubleshoot errors in the Country class.

            Arguments:
            country_code -- the three-letter unique code for the country to be added
            country_name -- the name of the country to be added
        """

        with self.engine.connect() as conn:
            conn.execute(
                insert(Country)
                .values(code=country_code, name=country_name)
            )
            conn.commit()

    def select_country(self, country_id) -> Country:
        """ An internal helper method to extract a specified Country record without going through the
            Country class.

            Arguments:
            country_id -- the country code for the record to return
        """

        with Session(self.engine) as session:
            my_country = session.execute(
                select(Country)
                .where(Country.code == country_id)
            ).first()

        return my_country[0] if my_country else None

#region Overhead

    def test_init(self):
        """ Test the constructor for the Country class. """

        my_country = Country(code='ABC', name='alphabet')
        self.assertEqual('ABC', my_country.code)
        self.assertEqual('alphabet', my_country.name)

    def test_eq(self):
        """ Test the equality operation for the Country class. """

        first_country = Country(code='ABC', name='alphabet')
        second_country = Country(code='ABC', name='alphabet')
        third_country = Country(code='ABCD', name='alphabet')
        fourth_country = Country(code='ABC', name='aLpHaBeT')

        self.assertEqual(first_country, second_country)
        self.assertNotEqual(first_country, third_country)
        self.assertNotEqual(first_country, fourth_country)
        self.assertNotEqual(first_country, None)

    def test_repr(self):
        """ Test the to-string behaviour for the Country class. """

        my_country = Country(code='ABC', name='alphabet')
        self.assertEqual("Country(code='ABC', name='alphabet')", str(my_country))

#endregion

#region Create operations

    def test_create_record(self):
        """ Test the success case for the `create_record()` function. """

        exp_country = Country(code='ABC', name='alphabet')

        # Create a new record in the database, then extract it
        Country.create_record(self.engine, 'ABC', 'alphabet')
        obs_country = self.select_country('ABC')

        self.assertEqual(exp_country, obs_country)

    def test_create_record_fail(self):
        """ Test the fail case for the `create_record()` function when the country code is
            not the accepted length.
        """

        for invalid_code in ['AB', 'ABCD']:
            with self.assertRaises(ValueError) as error_context:
                Country.create_record(self.engine, invalid_code, 'alphabet')

            self.assertEqual(
                f"Country code '{invalid_code}' must be three characters long.",
                str(error_context.exception)
            )

#endregion

#region Select operations

    def test_select_all(self):
        """ Test the behaviour of the `select_all()` function when there are entries in the
            database.
        """

        # Set the expected values and insert into the database
        exp_records = [
            Country(code='ABC', name='name_1'),
            Country(code='DEF', name='name_2'),
            Country(code='GHI', name='name_3'),
        ]

        for country in exp_records:
            self.insert_country(country.code, country.name)

        # Extract the database records and compare to the original inputs
        obs_records = Country.select_all(self.engine)
        self.assertListEqual(exp_records, obs_records)

    def test_select_all_empty(self):
        """ Test the behaviour of the `select_all()` function when there are not entries in
            the database.
        """

        obs_records = Country.select_all(self.engine)
        self.assertListEqual([], obs_records)

    def test_select_by_id(self):
        """ Test the behaviour of the `select_by_id()` function when there is a match in the
            database.
        """

        exp_country = Country(code='ABC', name='alphabet')

        self.insert_country(exp_country.code, exp_country.name)
        obs_country = Country.select_by_id(self.engine, exp_country.code)

        self.assertEqual(exp_country, obs_country)

    def test_select_by_id_empty(self):
        """ Test the behaviour of the `select_by_id()` function when there is no match in the
            database.
        """

        obs_country = Country.select_by_id(self.engine, 'ABC')
        self.assertIsNone(obs_country)

#endregion