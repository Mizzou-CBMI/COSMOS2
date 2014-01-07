from sqlalchemy import create_engine, event
from sqlalchemy.orm import scoped_session, sessionmaker, mapper
from sqlalchemy.ext.declarative import declarative_base
from collections import OrderedDict
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

import os, sys
from . import settings

DEFAULT_ENGINE_URL = 'sqlite:////' + os.path.join(settings['app_store_path'], 'sqlite.db')

engine = create_engine(DEFAULT_ENGINE_URL, echo=False)
Session = sessionmaker(autocommit=False,
                       autoflush=False,
                       bind=engine)
session = Session()

#http://docs.sqlalchemy.org/en/rel_0_8/dialects/sqlite.html#foreign-key-support
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

class Base(declarative_base()):
    __abstract__ = True
    exclude_from_dict = []
    def attrs_as_dict(self):
        l = [(c.name, getattr(self, c.name)) for c in self.__table__.columns if c.name != 'id' and c.name not in self.exclude_from_dict]
        return OrderedDict(l)

    @property
    def session(self):
        return inspect(self).session

    @property
    def query(self):
        return self.session.query(self.__class__)

def initdb():
    Base.metadata.create_all(bind=session.bind)


def resetdb():
    print >> sys.stderr, 'Resetting db..'
    Base.metadata.drop_all(bind=session.bind)
    initdb()