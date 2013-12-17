import os
settings = dict(
    library_path=os.path.dirname(__file__)
)

from models.TaskFile import TaskFile
from models.Task import Task, INPUT
from rel import one2many, one2one, many2many, many2one
from models.TaskGraph import TaskGraph, Stage
from kosmos.drm.runner import run