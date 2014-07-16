__version__ = '0.6'
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import sys

from .db import Base








# #######################################################################################################################
# Settings
# #######################################################################################################################
import os

opj = os.path.join

library_path = os.path.dirname(os.path.realpath(__file__))


def default_get_submit_args(drm, task, default_queue=None):
    """
    Default method for determining the arguments to pass to the drm specified by :param:`drm`

    :returns: (str) arguments.  For example, returning "-n 3" if :param:`drm` == 'lsf' would caused all jobs
      to be submitted with bsub -n 3.  Returns None if no native_specification is required.
    """

    cpu_req = task.cpu_req
    mem_req = task.mem_req
    time_req = task.time_req

    if 'lsf' in drm:
        return '-R "rusage[mem={mem}] span[hosts=1]" -n {cpu}{time}{queue} -J "{jobname}"'.format(mem=(mem_req or 0) / cpu_req,
                                                                                                cpu=cpu_req,
                                                                                                time=' -W 0:{0}'.format(time_req) if time_req else '',
                                                                                                queue=' -q %s' % default_queue if default_queue else '',
                                                                                                jobname='%s_task(%s)' % (task.stage.name, task.id))
    elif 'ge' in drm:
        # return '-l h_vmem={mem_req}M,num_proc={cpu_req}'.format(
        return '-pe smp {cpu_req}{queue}'.format(mem_req=mem_req,
                                                cpu_req=cpu_req,
                                                queue=' -q %s' % default_queue if default_queue else '')
    elif drm == 'local':
        return None
    else:
        raise Exception('DRM not supported')


class Kosmos(object):
    def __init__(self, database_url, get_submit_args=default_get_submit_args, default_queue=None, flask_app=None):
        """

        :param database_url: a sqlalchemy database url.  ex: sqlite:///home/user/sqlite.db or mysql://user:pass@localhost/insilico
        :param get_submit_args: a function that returns arguments to be passed to the job submitter, like resource requirements or the queue to submit to.
            see :func:`default_get_submit_args` for details
        :param flask_app: a Flask application instance for the web interface.  The default behavior is to create one.
        """
        if '://' not in database_url:
            if database_url[0] != '/':
                # database_url is a relative path
                database_url = 'sqlite:///%s/%s' % (os.getcwd(), database_url)
            else:
                database_url = 'sqlite:///%s' % database_url

        self.flask_app = flask_app if flask_app else Flask(__name__)
        self.get_submit_args = get_submit_args
        self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        self.sqla = SQLAlchemy(self.flask_app)
        self.session = self.sqla.session
        self.default_queue = default_queue

    def initdb(self):
        """
        Initialize the database via sql CREATE statements
        """
        print >> sys.stderr, 'Initializing db...'
        Base.metadata.create_all(bind=self.session.bind)

    def resetdb(self):
        """
        Resets the database.  This is not reversible!
        """
        print >> sys.stderr, 'Dropping tables in db...'
        Base.metadata.drop_all(bind=self.session.bind)
        self.initdb()

    def shell(self):
        """
        Launch an IPython shell with useful variables already imported
        """
        kosmos_app = self
        session = self.session
        executions = self.session.query(Execution).all()
        ex = executions[-1] if len(executions) else None

        import IPython

        IPython.embed()

    def runweb(self, host, port):
        return self.flask_app.run(debug=True, host=host, port=port)


# #######################################################################################################################
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
    killed = 'Manually killed'
    failed_but_running = 'Failed, but running'
    failed = 'Finished, but failed'


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
from .models.TaskFile import TaskFile, output_taskfile, input_taskfile
from .models.Task import Task
from .models import rel
from .models.Stage import Stage
from .models.Tool import Tool, Input, Inputs
from .models.Execution import Execution
from .util.args import add_execution_args
from .models.Tool import collapse_tools
# from .db import get_session


__all__ = ['rel', 'Recipe', 'TaskFile', 'Task', 'Inputs', 'rel', 'Stage', 'Execution', 'TaskStatus', 'StageStatus',
           'Tool', 'collapse_tools']