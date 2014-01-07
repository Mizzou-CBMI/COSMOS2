from sqlalchemy import create_engine, event
from sqlalchemy.orm import scoped_session, sessionmaker, mapper
from sqlalchemy.ext.declarative import declarative_base
from collections import OrderedDict
from sqlalchemy import inspect

import os, sys
from . import settings

DEFAULT_ENGINE_URL = 'sqlite:////' + os.path.join(settings['app_store_path'], 'sqlite.db')

engine = create_engine(DEFAULT_ENGINE_URL, echo=False)
Session = sessionmaker(autocommit=False,
                       autoflush=False,
                       bind=engine)
session = Session()
Base = declarative_base()
Base.as_dict = lambda r: OrderedDict([(c.name, getattr(r, c.name)) for c in r.__table__.columns])
#Base.session = property(lambda self: inspect(self).session)
#Base.query = property(lambda o: session.query(o.__class__))


def initdb():
    Base.metadata.create_all(bind=session.bind)


def resetdb():
    print >> sys.stderr, 'Resetting db..'
    Base.metadata.drop_all(bind=session.bind)
    initdb()