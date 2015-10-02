import os
from cosmos.api import Cosmos
cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
cosmos.runweb('0.0.0.0', 5151)