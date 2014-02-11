__version__ = '0.1'

########################################################################################################################
# Settings
########################################################################################################################
import os
from collections import defaultdict

opj = os.path.join

settings = dict(
    library_path=os.path.dirname(os.path.realpath(__file__))
)


########################################################################################################################
# Misc
########################################################################################################################

class ExecutionFailed(Exception): pass

########################################################################################################################
# Signals
########################################################################################################################
import blinker

signal_task_status_change = blinker.Signal()
signal_stage_status_change = blinker.Signal()
signal_execution_status_change = blinker.Signal()


########################################################################################################################
# Enums
########################################################################################################################
import enum

class MyEnum(enum.Enum):
    def __str__(self):
        return "%s" % (self._value_)

class TaskStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    waiting = 'Waiting to execute',
    submitted = 'Submitted to the job manager',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually killed'


class StageStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    running = 'Stage is running',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually killed'


class ExecutionStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    running = 'Execution is running',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually killed'


class RelationshipType(MyEnum):
    one2one = 'one2one',
    one2many = 'one2many',
    many2one = 'many2one',
    many2many = 'many2many'

########################################################################################################################
# Imports
########################################################################################################################

from .models import rel
from .models.Recipe import Recipe, stagegraph_to_agraph
from .models.TaskFile import TaskFile
from .models.Task import Task
from .models import rel
from .models.Stage import Stage
from .models.Tool import Tool, Input, Inputs
from .models.Execution import Execution
from .util.args import add_execution_args, parse_and_start, default_argparser
from .db import get_session


__all__ = ['rel', 'Recipe', 'TaskFile', 'Task', 'Inputs', 'rel', 'Stage', 'Execution', 'TaskStatus', 'StageStatus',
           'Tool']