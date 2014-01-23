__version__ = '0.1'

########################################################################################################################
# Settings
########################################################################################################################
import os
import sys
from collections import defaultdict

opj = os.path.join
app_store_path = os.path.expanduser('~/.kosmos')
if not os.path.exists(app_store_path):
    os.mkdir(app_store_path)

settings = dict(
    library_path=os.path.dirname(__file__),
    app_store_path=app_store_path
)
conf_path = opj(app_store_path, 'kosmos.conf')
if os.path.exists(conf_path):
    from configparser import ConfigParser
    configp = ConfigParser()
    configp.read(conf_path)
    config = defaultdict(lambda: None, configp.values()[1].items())
else:
    config = defaultdict(lambda: None)

########################################################################################################################
# Misc
########################################################################################################################
from collections import namedtuple

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


class TaskStatus(enum.Enum):
    no_attempt = 'Has not been attempted',
    waiting = 'Waiting to execute',
    submitted = 'Submitted to the job manager',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually Killed'


class StageStatus(enum.Enum):
    no_attempt = 'Has not been attempted',
    running = 'Stage is running',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually Killed'


class ExecutionStatus(enum.Enum):
    no_attempt = 'Has not been attempted',
    running = 'Execution is running',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually Killed'


########################################################################################################################
# Imports
########################################################################################################################

from .models import rel
from .models.Recipe import Recipe
from .models.TaskFile import TaskFile
from .models.Task import Task, INPUT
from .models import rel
from .models.Stage import Stage
from .models.Execution import Execution
from .util.args import add_execution_args, parse_and_start, default_argparser
from .db import get_session


__all__ = ['rel', 'Recipe', 'TaskFile', 'Task', 'INPUT', 'rel', 'Stage', 'Execution', 'TaskStatus', 'StageStatus']