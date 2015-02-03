from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from collections import OrderedDict
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQLite3Connection
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.schema import Column
from sqlalchemy.types import String, Integer


# def get_session(database_url=None, echo=False):
#     """
#     :returns: a sqlalchemy session
#     """
#     if database_url is None:
#         raise ValueError('database_url cannot be None.')
#     engine = create_engine(database_url, echo=echo)
#     session_factory = sessionmaker(autocommit=False,
#                                    autoflush=False,
#                                    bind=engine)
#     Session = scoped_session(session_factory)
#     return Session


#http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#foreign-key-support
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Turn on sqlite foreignkey support"""
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Base(declarative_base()):
    __abstract__ = True
    exclude_from_dict = []

    def attrs_as_dict(self):
        l = [(c.name, getattr(self, c.name)) for c in self.__table__.columns if
             c.name != 'id' and c.name not in self.exclude_from_dict]
        return OrderedDict(l)

    @property
    def session(self):
        return inspect(self).session

    @property
    def query(self):
        return self.session.query(self.__class__)

class MetaData(Base):
    __tablename__ = 'metadata'
    id = Column(Integer, primary_key=True)
    initdb_library_version = Column(String(255))