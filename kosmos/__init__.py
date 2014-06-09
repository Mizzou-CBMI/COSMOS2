__version__ = '0.6'
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

from .db import Base
import sys

########################################################################################################################
# Settings
########################################################################################################################
import os
from collections import defaultdict

opj = os.path.join

library_path = os.path.dirname(os.path.realpath(__file__))

from flask.signals import request_started


def default_get_drmaa_native_specification(drm, task):
    """
    Default method for determining the arguments to pass to the drm specified by :param:`drm`

    :returns: (str) arguments.  For example, returning "-n 3" if :param:`drm` == 'lsf' would caused all jobs
      to be submitted with bsub -n 3.  Returns None if no native_specification is required.
    """

    cpu_req = task.cpu_req
    mem_req = task.mem_req
    time_req = task.time_req

    if 'lsf' in drm:
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format((mem_req or 0) / cpu_req, cpu_req)
        if time_req:
            s += ' -W 0:{0}'.format(time_req)
            # if queue:
        #     s += ' -q {0}'.format(queue)
        return s
    elif 'ge' in drm:
        #return '-l h_vmem={mem_req}M,num_proc={cpu_req}'.format(
        return '-l cpu={cpu_req}'.format(
            mem_req=mem_req,
            cpu_req=cpu_req)
    elif drm == 'local':
        return None
    else:
        raise Exception('DRM not supported')


class KosmosApp(object):
    def __init__(self, database_url, get_drmaa_native_specification=default_get_drmaa_native_specification, flask_app=None):
        self.flask_app = flask_app if flask_app else Flask(__name__)
        self.get_drmaa_native_specification = get_drmaa_native_specification
        self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        self.sqla = SQLAlchemy(self.flask_app)
        self.session = self.sqla.session

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

    # def runweb(self, host, port):
    #     from .web.views import gen_bprint
    #     from .web import filters
    #     from kosmos.web.admin import add_kosmos_admin
    #     #print flask_app.url_map
    #     self.flask_app.register_blueprint(gen_bprint(self), url_prefix='/kosmos')
    #     self.flask_app.config['DEBUG'] = True
    #     self.flask_app.secret_key = '\x07F\xdd\x98egfd\xc1\xe5\x9f\rv\xbe\xdbl\x93x\xc2\x19\x9e\xc0\xd7\xea'
    #     add_kosmos_admin(self.flask_app, self.sqla.session)
    #
    #     return self.flask_app.run(debug=True, host=host, port=port)


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
    killed = 'Manually killed'
    failed_but_running = 'At least one task that must succeed has failed, but still running non-dependent jobs until completion'
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
# from .db import get_session


__all__ = ['rel', 'Recipe', 'TaskFile', 'Task', 'Inputs', 'rel', 'Stage', 'Execution', 'TaskStatus', 'StageStatus',
           'Tool']