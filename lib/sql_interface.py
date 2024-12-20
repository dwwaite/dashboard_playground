from typing import Dict, List
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import Engine

from lib.sql_country import Country
from lib.sql_geotag import GeoTag
from lib.sql_gdelt_record import GdeltRecord
from lib.sql_mapping import BASE

class DataInterface():

    @staticmethod
    def exp_tables() -> List[str]:
        return [
            GdeltRecord.__tablename__,
            GeoTag.__tablename__,
            Country.__tablename__,
        ]

    @staticmethod
    def open_connection(db_path: str, echo: bool=False) -> Engine:
        """ Standardised Engine instantiation.

            Arguments:
            db_path -- the file location at which the database is to be created
            echo    -- (optional) use echoing for SQL transactions
        """

        return create_engine(f"sqlite+pysqlite:///{db_path}", echo=echo, future=True)

    @staticmethod
    def create_blank_database(connection: str) -> Engine:
        """ Build the database structure to an existing connection (file or memory)

            Arguments:
            connection -- the connection at which the database is to be created
        """

        BASE.metadata.create_all(connection)

    @staticmethod
    def export_table_columns(engine: Engine, table_name: str) -> List[str]:
        """ Export the column names of a user-specified table available in an active connection.
            This function only returns the names of the columns, not the actual representation
            of them.

            Arguments:
            engine     -- an open connection to a database
            table_name -- the name of the table to be retrieved
        """

        table_map = DataInterface.map_database(engine)
        return [x.name for x in table_map[table_name].columns]

    @staticmethod
    def map_database(engine: Engine) -> Dict[str, Table]:
        """ Reflect the database provided through a user-specified connection and map the results
            in key/value pairs.

            Arguments:
            engine -- an open connection to a database
        """

        # Open and reflect the database
        metadata = MetaData()
        metadata.reflect(bind=engine)

        return {name: table for (name, table) in metadata.tables.items()}

    @staticmethod
    def report_database(engine: Engine) -> None:
        """ Iterate through the database provided at the user-specified path and report the presence
                of each table. Tables are categorised as expected or unexpected, although there are
                no restrictions on their columns.

            Arguments:
            engine -- an open connection to a database
        """

        exp_tables = DataInterface.exp_tables()
        table_map = DataInterface.map_database(engine)

        # Iterate over the contents, comparing against a checklist of expected tables
        for name, table in table_map.items():

            if name in exp_tables:
                print(f"Table: {name}")
                for column in table.columns:
                    print(f"{'PK' if column.primary_key else '  '}  {column.name} ({column.type})")

                exp_tables.remove(name)

            else:
                print(f"ERROR: Observed table {name} is not in the expected list.")

        for exp_table in exp_tables:
            print(f"ERROR: Expected table {exp_table} was not found in the collection.")
