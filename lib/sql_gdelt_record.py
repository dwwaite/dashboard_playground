from datetime import datetime
from typing import Any, Dict, List, Optional
import polars as pl
from sqlalchemy import Date, Float, ForeignKey, Integer, String, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from lib.sql_country import Country
from lib.sql_geotag import GeoTag
from lib.sql_mapping import BASE

class GdeltRecord(BASE):
    """ Mapped class for the GDELT reduced (v2) table. """

    __tablename__ = 'GDELT_RECORD'

    record_key: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[Date] = mapped_column(Date)
    source_id: Mapped[String] = mapped_column(String, ForeignKey('COUNTRY.code'))
    target_id: Mapped[String] = mapped_column(String, ForeignKey('COUNTRY.code'))
    cameo_code: Mapped[Integer] = mapped_column(Integer)
    num_events: Mapped[Integer] = mapped_column(Integer)
    num_arts: Mapped[Integer] = mapped_column(Integer)
    quad_class: Mapped[Float] = mapped_column(Float)
    goldstein: Mapped[Optional[Float]] = mapped_column(Float)
    source_record_id: Mapped[Optional[Integer]] = mapped_column(Integer, ForeignKey('GEO_TAG.geo_id'))
    target_record_id: Mapped[Optional[Integer]] = mapped_column(Integer, ForeignKey('GEO_TAG.geo_id'))
    action_record_id: Mapped[Optional[Integer]] = mapped_column(Integer, ForeignKey('GEO_TAG.geo_id'))

    source: Mapped['Country'] = relationship('Country', foreign_keys=source_id)
    target: Mapped['Country'] = relationship('Country', foreign_keys=target_id)
    source_record: Mapped['GeoTag'] = relationship('GeoTag', foreign_keys=source_record_id)
    target_record: Mapped['GeoTag'] = relationship('GeoTag', foreign_keys=target_record_id)
    action_record: Mapped['GeoTag'] = relationship('GeoTag', foreign_keys=action_record_id)

    def __eq__(self, other_gdelt) -> bool:

        match_values = [
            self.date == other_gdelt.date,
            self.source_id == other_gdelt.source_id,
            self.target_id == other_gdelt.target_id,
            self.cameo_code == other_gdelt.cameo_code,
            self.num_events == other_gdelt.num_events,
            self.num_arts == other_gdelt.num_arts,
            self.quad_class == other_gdelt.quad_class,
            self.goldstein == other_gdelt.goldstein,
            self.source_record_id == other_gdelt.source_record_id,
            self.target_record_id == other_gdelt.target_record_id,
            self.action_record_id == other_gdelt.action_record_id,
        ]

        return sum(match_values) == len(match_values)

    def __repr__(self) -> str:
        return (
            "GdeltRecord("
            f"date={self.date!r}, source_id={self.source_id!r}, target_id={self.target_id!r}, "
            f"cameo_code={self.cameo_code!r}, num_events={self.num_events!r}, num_arts={self.num_arts!r}, "
            f"quad_class={self.quad_class!r}, goldstein={self.goldstein!r}, "
            f"source_record_id={self.source_record_id!r}, target_record_id={self.target_record_id!r}, "
            f"action_record_id={self.action_record_id!r}"
            ")"
        )

    @staticmethod
    def _resolve_country_entry(engine: Engine, value: Any) -> Country:
        """ Extracts the value encoded mapped to the insert key and evaluates the result.
            Returns either the Country record contained in the kwargs, or the Country
            record represented by the country code contained in the kwargs.

            Arguments:
            engine -- an object of type Engine referencing the current database
            value  -- the value to be evaluated for return
        """

        return value if isinstance(value, Country) else Country.select_by_id(engine, value)

    @staticmethod
    def create_record(
        engine: Engine,
        date: datetime,
        source: Any,
        target: Any,
        cameo_code: int,
        num_events: int,
        num_arts: int,
        quad_class: int,
        goldstein: float=None,
        source_record: GeoTag=None,
        target_record: GeoTag=None,
        action_record: GeoTag=None,
    ) -> None:
        """ Insert a record into the database. All user-controlled data is set via keyword arguments
            to allow for easy code writting. In practice, this is not allowed from the dashboard and
            only exists for backend setup.

            Arguments:
            engine        -- an object of type Engine referencing the current database
            date          -- the date stamp for the record being inserted, with or without time information
            source        -- either an instance of type Country, or the country code, for the source of the event
            target        -- either an instance of type Country, or the country code, for the target of the event
            cameo_code    -- the CAMEO code describing the event
            num_events    -- the number of events recorded
            num_arts      -- the number of articles recorded
            quad_class    -- the primary classification of the CAMEO code
            goldstein     -- (optional) the Goldstein Scale score for the CAMEO event code
            source_record -- (optional) A GeoTag record for the source of the event
            target_record -- (optional) A GeoTag record for the target of the event
            action_record -- (optional) A GeoTag record for the action described by the event
        """

        new_record = GdeltRecord(
            date=date,
            source=GdeltRecord._resolve_country_entry(engine, source),
            target=GdeltRecord._resolve_country_entry(engine, target),
            cameo_code=cameo_code,
            num_events=num_events,
            num_arts=num_arts,
            quad_class=quad_class,
            goldstein=goldstein,
            source_record=source_record,
            target_record=target_record,
            action_record=action_record,
        )

        with Session(engine) as session:
            session.add(new_record)
            session.commit()

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
                insert(GdeltRecord),
                records,
            )
            session.commit()

    @staticmethod
    def select_all(engine: Engine) -> pl.DataFrame:
        """ Return a list of all GdeltRecord records in the database, formatted as a polars
            DataFrame.

            Arguments:
            engine -- an object of type Engine referencing the current database
        """

        return (
            pl
            .read_database(
                query="""
                    SELECT date, source_id, target_id, cameo_code, num_events, num_arts, quad_class, goldstein, source_record_id, target_record_id, action_record_id
                    FROM GDELT_RECORD
                """,
                connection=engine
            )
            .cast({'date': pl.Date})
        )

    @staticmethod
    def select_by_country(engine: Engine, source_country: Country, target_country: Country=None) -> pl.DataFrame:
        """ Return a list of all GdeltRecord records in the database originating from the country
            specified by the user. Optionally may also restrict to a target country of interest (i.e
            articles from X, or articles from X about Y).

            Results are captured as a polars DataFrame for ease of visualisation.

            Arguments:
            engine         -- an object of type Engine referencing the current database
            source_country -- the country from which records originate
            target_country -- the country which records the records are about
        """

        query_string = """
            SELECT date, source_id, target_id, cameo_code, num_events, num_arts, quad_class, goldstein, source_record_id, target_record_id, action_record_id
            FROM GDELT_RECORD
            WHERE source_id == :source_country
        """
        query_params = {'source_country': source_country.code}

        if target_country:
            query_string += """ AND target_id == :target_country """
            query_params['target_country'] = target_country.code

        return (
            pl
            .read_database(
                query=query_string,
                connection=engine,
                execute_options={'parameters': query_params}
            )
            .cast({'date': pl.Date})
        )
