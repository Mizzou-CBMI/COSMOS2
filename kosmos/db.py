from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from collections import OrderedDict
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

import sys


def get_session(database_url=None, echo=False):
    """
    :returns: a sqlalchemy session
    """
    if database_url is None:
        #database_url ='sqlite:////' + os.path.join(settings['app_store_path'], 'sqlite.db')
        raise ValueError('database_url cannot be None.')
    engine = create_engine(database_url, echo=echo)
    Session = sessionmaker(autocommit=False,
                           autoflush=False,
                           bind=engine)
    return Session()


#http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#foreign-key-support
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Turn on sqlite foreignkey support"""
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


def initdb(database_url=None, echo=True):
    """
    Initialize the database via sql CREATE statements
    """
    session = get_session(database_url, echo=echo)
    Base.metadata.create_all(bind=session.bind)
    return session


def resetdb(database_url=None, echo=True):
    """
    Resets the database.  This is not reversible!
    """
    print >> sys.stderr, 'Resetting db..'
    session = get_session(database_url, echo=echo)
    Base.metadata.drop_all(bind=session.bind)
    initdb(database_url=database_url)
    return session