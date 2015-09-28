import os
import math

# turn SQLAlchemy warnings into errors
import warnings
from sqlalchemy.exc import SAWarning

warnings.simplefilter("error", SAWarning)

opj = os.path.join

#########################################################################################################################
# Settings
#########################################################################################################################

library_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(library_path, 'VERSION'), 'r') as fh:
    __version__ = fh.read().strip()


#########################################################################################################################
# Misc
#########################################################################################################################

class ExecutionFailed(Exception): pass

#########################################################################################################################
# Signals
#########################################################################################################################
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
        return "%s" % self._value_


NOOP = None


class TaskStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    waiting = 'Waiting to execute',  # deprecated
    submitted = 'Submitted to the job manager',
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually killed'


class StageStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    running = 'Running',
    running_but_failed = 'Running, but a task failed'
    successful = 'Finished successfully',
    failed = 'Finished, but failed'
    killed = 'Manually killed'


class ExecutionStatus(MyEnum):
    no_attempt = 'No Attempt yet',
    running = 'Running',
    successful = 'Successfully Finished',
    killed = 'Killed'
    failed_but_running = "Running, but a task failed"
    failed = 'Failed, but finished'


class RelationshipType(MyEnum):
    one2one = 'one2one',
    one2many = 'one2many',
    many2one = 'many2one',
    many2many = 'many2many'
