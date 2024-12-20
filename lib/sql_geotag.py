from typing import Any, Dict, List
import polars as pl
from sqlalchemy import Float, Integer
from sqlalchemy import insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import Session

from lib.sql_mapping import BASE

class GeoTag(BASE):
    """ Mapped class for the GeoTag table, which records the Source/Target/Action triplets. """

    __tablename__ = 'GEO_TAG'

    geo_id: Mapped[Integer] = mapped_column(Integer, primary_key=True)
    geo_type: Mapped[Integer] = mapped_column(Integer)
    geo_lat: Mapped[Float] = mapped_column(Float)
    geo_long: Mapped[Float] = mapped_column(Float)

    def __repr__(self) -> str:
        return f"GeoTag(geo_type={self.geo_type!r}, geo_lat={self.geo_lat!r}, geo_long={self.geo_long!r})"

    def __eq__(self, other_tag) -> bool:
        return self.geo_type == other_tag.geo_type and self.geo_lat == other_tag.geo_lat and self.geo_long == other_tag.geo_long

    @staticmethod
    def _find_existing_record(engine: Engine, geo_type: int, geo_lat: float, geo_long: float) -> int:
        """ Searches the database for an existing record match the type/latitude/longitude of the parameters.
            Returns the PK on a success or None if there is no record.

            Arguments:
            engine   -- an object of type Engine referencing the current database
            geo_type -- an integer value representing the record type
            geo_lat  -- the latitide of the record
            geo_long -- the longitude of the record
        """

        with Session(engine) as session:

            geo_record = session.execute(
                select(GeoTag)
                .where(GeoTag.geo_type == geo_type)
                .where(GeoTag.geo_lat == geo_lat)
                .where(GeoTag.geo_long == geo_long)
            ).first()

        return geo_record[0].geo_id if geo_record else None

    @staticmethod
    def create_new_record(engine: Engine, geo_type: int, geo_lat: float, geo_long: float) -> int:
        """ Insert a new GeoTag record into the database if an identical match does not already exist.
            Returns either the PK of an existing identical record, or the newly created record.

            Arguments:
            engine   -- an object of type Engine referencing the current database
            geo_type -- an integer value representing the record type
            geo_lat  -- the latitide of the record
            geo_long -- the longitude of the record
        """

        if (geo_type is None) and (geo_lat is None) and (geo_long is None):
            return None

        elif existing_id := GeoTag._find_existing_record(engine, geo_type, geo_lat, geo_long):
            return existing_id

        else:
            new_record = GeoTag(geo_type=geo_type, geo_lat=geo_lat, geo_long=geo_long)

            # This is the Session equivalent of a returning statement
            with Session(engine) as session:
                session.add(new_record)
                session.commit()
                session.refresh(new_record)

            return new_record.geo_id

    @staticmethod
    def create_mass_records(engine: Engine, records: List[Dict[str, Any]]) -> None:
        """ Bulk-insert records into the database. Accepts a list of dictionaries, each mapping
            the table column names. In practice, this is not allowed from the dashboard and
            only exists for backend setup.

            Arguments:
            engine  -- an object of type Engine referencing the current database
            records -- a list of dictionary representations of the data to insert
        """

        with Session(engine) as session:
            session.execute(
                insert(GeoTag),
                records,
            )
            session.commit()

    @staticmethod
    def select_all(engine: Engine) -> pl.DataFrame:
        """ Return a list of all GeoTag records in the database, formatted as a polars
            DataFrame.

            Arguments:
            engine -- an object of type Engine referencing the current database
        """

        with Session(engine) as session:
            geotags = session.scalars(select(GeoTag)).all()

        return pl.DataFrame([
            {'Geo_ID': geotag.geo_id, 'GeoType': geotag.geo_type, 'GeoLat': geotag.geo_lat, 'GeoLong': geotag.geo_long}
            for geotag in geotags
        ])

    @staticmethod
    def select_by_id(engine: Engine, geo_id: int) -> 'GeoTag':
        """ Return specific GeoTag instance according to the user specified key. This will typically
            only be known when using values from the GdeltReduced table.

            Arguments:
            engine -- an object of type Engine referencing the current database
            geo_id -- the GeoTag identifier for the record
        """

        with Session(engine) as session:
            geotag_record = session.execute(
                select(GeoTag)
                .where(GeoTag.geo_id == geo_id)
            ).first()

        return geotag_record[0] if geotag_record else None
