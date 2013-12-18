import os
settings = dict(
    library_path=os.path.dirname(__file__)
)
# from .utils import apipkg
#
# apipkg.initpkg(__name__, dict(
#     Task='.models.Task:Task',
#     TaskFile='.models.TaskFile:TaskFile',
#     INPUT='.models.TaskFile:INPUT',
#     rel='.models.rel',
#     TaskGraph='.models.TaskGraph:TaskGraph',
#     Stage='.models.Stage:Stage',
#     run='.drm.taskgraph_runner:run',
# ), locals())

from models.TaskFile import TaskFile
from models.Task import Task, INPUT
from kosmos.models import rel
from models.TaskGraph import TaskGraph
from models.Stage import Stage
from kosmos.drm.taskgraph_runner import run