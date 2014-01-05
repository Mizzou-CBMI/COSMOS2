########################################################################################################################
# Settings
########################################################################################################################
import os

app_store_path = os.path.expanduser('~/.kosmos')
if not os.path.exists(app_store_path):
    os.mkdir(app_store_path)

settings = dict(
    library_path=os.path.dirname(__file__),
    app_store_path=app_store_path
)


########################################################################################################################
# Signals
########################################################################################################################
import blinker

signal_task_status_change = blinker.Signal()
signal_stage_status_change = blinker.Signal()


########################################################################################################################
# Enums
########################################################################################################################
import enum
class TaskStatus(enum.Enum):
    no_attempt='Has not been attempted',
    waiting='Waiting to execute',
    submitted='Submitted to the job manager',
    successful='Finished successfully',
    failed='Finished, but failed'

class StageStatus(enum.Enum):
    no_attempt='Has not been attempted',
    running='Submitted to the job manager',
    finished='All Tasks have finished'

########################################################################################################################
# Imports
########################################################################################################################

from .models import rel
from .models.Recipe import Recipe
from .models.TaskFile import TaskFile
from .models.Task import Task, INPUT
from .models import rel
from .models.TaskGraph import TaskGraph
from .models.Stage import Stage
from .models.Execution import Execution


__all__ = ['rel','Recipe','TaskFile','Task','INPUT','rel','TaskGraph','Stage','Execution', 'TaskStatus', 'StageStatus']