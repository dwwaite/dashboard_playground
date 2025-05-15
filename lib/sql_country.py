from typing import List
from sqlalchemy import String
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import Mapped, mapped_column

from lib.sql_mapping import BASE

class Country(BASE):
    """ Mapped class for the country table. """

    __tablename__ = 'COUNTRY'

    code: Mapped[String] = mapped_column(String(3), primary_key=True)
    name: Mapped[String] = mapped_column(String(50))

    def __repr__(self) -> str:
        return f"Country(code={self.code!r}, name={self.name!r})"

    def __eq__(self, other_country) -> bool:
        if type(other_country) == Country:
            return self.code == other_country.code and self.name == other_country.name
        else:
            return False

    @staticmethod
    def create_record(engine: Engine, country_code: str, country_name: str) -> None:
        """ Insert a record into the database. Ensures that the unique (PK) country code is three
            characters in length before attemtping to commit. Raises a ValueError if the code is
            of incorrect length.

            Arguments:
            engine       -- an object of type Engine referencing the current database
            country_code -- a three-character code uniquely representing the country
            country_name -- the full name for the country being recorded
        """

        if len(country_code) != 3:
            raise ValueError(f"Country code '{country_code}' must be three characters long.")

        new_record = Country(code=country_code, name=country_name)

        with Session(engine) as session:
            session.add(new_record)
            session.commit()

    @staticmethod
    def select_all(engine: Engine) -> List['Country']:
        """ Return a list of all Country records in the database, formatted as a polars
            DataFrame.

            Arguments:
            engine -- an object of type Engine referencing the current database
        """

        with Session(engine) as session:
            country_records = session.scalars(select(Country)).all()

        return country_records

    @staticmethod
    def select_by_id(engine: Engine, country_id: str) -> 'Country':
        """ Return specific country instance according to the user specified key.

            Arguments:
            engine     -- an object of type Engine referencing the current database
            country_id -- the 3-digit ID code for the country
        """

        with Session(engine) as session:
            country_record = session.execute(
                select(Country)
                .where(Country.code == country_id)
            ).first()

        return country_record[0] if country_record else None
