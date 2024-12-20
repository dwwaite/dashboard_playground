from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

@event.listens_for(Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record: Any):
    ''' Event listener to ensure FK relationships are enforced during database handling. '''

    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()

BASE = declarative_base()
