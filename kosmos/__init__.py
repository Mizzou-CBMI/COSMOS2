import os

app_store_path = os.path.expanduser('~/.kosmos')
if not os.path.exists(app_store_path):
    os.mkdir(app_store_path)


settings = dict(
    library_path=os.path.dirname(__file__),
    app_store_path=app_store_path
)

from .models import rel
from .models.Recipe import Recipe
from .models.TaskFile import TaskFile
from .models.Task import Task, INPUT
from .models import rel
from .models.TaskGraph import TaskGraph
from .models.Stage import Stage
from .models.Execution import Execution