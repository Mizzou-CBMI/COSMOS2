from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import sys
import os
import math
import itertools as it

from .util.helpers import get_logger, mkdir, confirm, str_format
from .util.args import get_last_cmd_executed
from .db import Base

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


def default_get_submit_args(task, default_queue=None, grid_engine_parallel_environment='smp'):
    """
    Default method for determining the extra arguments to pass to the DRM.
    For example, returning `"-n 3" if` `task.drm == "lsf"` would caused all jobs
    to be submitted with `bsub -n 3`.

    :param cosmos.Task task: The Task being submitted.
    :param default_queue: The default queue.
    :rtype: str
    """
    drm = task.drm or default_queue
    default_job_priority = None
    use_mem_req = False

    cpu_req = task.cpu_req
    mem_req = task.mem_req
    time_req = task.time_req

    jobname = '%s_task(%s)' % (task.stage.name, task.id)
    queue = ' -q %s' % default_queue if default_queue else ''
    priority = ' -p %s' % default_job_priority if default_job_priority else ''

    if drm == 'lsf':
        rusage = '-R "rusage[mem={mem}] ' if mem_req and use_mem_req else ''
        time = ' -W 0:{0}'.format(task.time_req) if task.time_req else ''
        return '-R "{rusage}span[hosts=1]" -n {task.cpu_req}{time}{queue} -J "{jobname}"'.format(**locals())

    elif drm == 'ge':
        mem_req_s = ' -l h_vmem=%sM' % int(math.ceil(mem_req / float(cpu_req))) if mem_req and use_mem_req else ''
        return '-pe {grid_engine_parallel_environment} {cpu_req}{queue}{mem_req_s}{priority} -N "{jobname}"'.format(**locals())
    elif drm == 'local':
        return None
    else:
        raise Exception('DRM not supported: %s' % drm)


#########################################################################################################################
# Cosmos Class
#########################################################################################################################

class Cosmos(object):
    def __init__(self,
                 database_url='sqlite:///:memory:',
                 get_submit_args=default_get_submit_args,
                 default_drm='local', default_queue=None,
                 flask_app=None):
        """
        :param str database_url: A `sqlalchemy database url <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_.  ex: sqlite:///home/user/sqlite.db or
            mysql://user:pass@localhost/database_name or postgresql+psycopg2://user:pass@localhost/database_name
        :param func get_submit_args: a function that returns arguments to be passed to the job submitter, like resource
            requirements or the queue to submit to.  See :func:`cosmos.default_get_submit_args` for details
        :param Flask flask_app: A Flask application instance for the web interface.  The default behavior is to create one.
        :param str default_drm: The Default DRM to use (ex 'local', 'lsf', or 'ge')
        """
        assert default_drm in ['local', 'lsf', 'ge'], 'unsupported drm: %s' % default_drm
        assert '://' in database_url, 'Invalid database_url: %s' % database_url

        if flask_app:
            self.flask_app = flask_app
        else:
            self.flask_app = Flask(__name__)
            self.flask_app.secret_key = os.urandom(24)

        self.get_submit_args = get_submit_args
        self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        self.sqla = SQLAlchemy(self.flask_app)
        self.session = self.sqla.session
        self.default_queue = default_queue
        self.default_drm = default_drm

        # setup flask views
        from cosmos.web.views import gen_bprint
        #from cosmos.web.admin import add_cosmos_admin

        self.cosmos_bprint = gen_bprint(self)
        self.flask_app.register_blueprint(self.cosmos_bprint)
        # add_cosmos_admin(flask_app, self.session)

    def start(self, name, output_dir=os.getcwd(), restart=False, skip_confirm=False, max_cpus=None, max_attempts=1,
              check_output_dir=True):
        """
        Start, resume, or restart an execution based on its name.  If resuming, deletes failed tasks.

        :param str name: A name for the workflow.  Must be unique for this Cosmos session.
        :param str output_dir: The directory to write files to.  Defaults to the current working directory.
        :param bool restart: If True and the execution exists, delete it first.
        :param bool skip_confirm: (If True, do not prompt the shell for input before deleting executions or files.
        :param int max_cpus: The maximum number of CPUs to use at once.
        :param int max_attempts: The maximum number of times to retry a failed job.
        :param bool check_output_dir: Raise an error if this is a new workflow, and output_dir already exists.

        :returns: An Execution instance.
        """
        assert os.path.exists(
            os.getcwd()), "The current working dir of this environment, %s, does not exist" % os.getcwd()
        output_dir = os.path.abspath(output_dir)
        output_dir = output_dir if output_dir[-1] != '/' else output_dir[0:]  # remove trailing slash
        prefix_dir = os.path.split(output_dir)[0]
        assert os.path.exists(prefix_dir), '%s does not exist' % prefix_dir
        from .util.helpers import mkdir

        session = self.session

        old_id = None
        if restart:
            ex = session.query(Execution).filter_by(name=name).first()
            if ex:
                old_id = ex.id
                msg = 'Restarting %s.  Are you sure you want to delete the contents of output_dir `%s` ' \
                      'and all sql records for this execution?' % (
                          ex.output_dir, ex)
                if not skip_confirm and not confirm(msg):
                    raise SystemExit('Quitting')

                ex.delete(delete_files=True)
            else:
                if not skip_confirm and not confirm('Execution with name %s does not exist, '
                                                    'but `restart` is set to True.  '
                                                    'Continue by starting a new Execution?' % name):
                    raise SystemExit('Quitting')

        # resuming?
        ex = session.query(Execution).filter_by(name=name).first()
        # msg = 'Execution started, Cosmos v%s' % __version__
        if ex:
            # resuming.
            if not skip_confirm and not confirm('Resuming %s.  All non-successful jobs will be deleted, '
                                                'then any new tasks in the graph will be added and executed.  '
                                                'Are you sure?' % ex):
                raise SystemExit('Quitting')
            assert ex.output_dir == output_dir, 'cannot change the output_dir of an execution being resumed.'

            ex.successful = False
            ex.finished_on = None

            if not os.path.exists(ex.output_dir):
                raise IOError('output_directory %s does not exist, cannot resume %s' % (ex.output_dir, ex))

            ex.log.info('Resuming %s' % ex)
            session.add(ex)
            failed_tasks = [t for s in ex.stages for t in s.tasks if not t.successful]
            n = len(failed_tasks)
            if n:
                ex.log.info('Deleting %s failed task(s) from SQL database, delete_files=%s' % (n, False))
                for t in failed_tasks:
                    session.delete(t)

            for stage in it.ifilter(lambda s: len(s.tasks) == 0, ex.stages):
                ex.log.info('Deleting stage %s, since it has no successful Tasks' % stage)
                session.delete(stage)

        else:
            # start from scratch
            if check_output_dir:
                assert not os.path.exists(output_dir), 'Execution output_dir `%s` already exists.' % (output_dir)

            mkdir(output_dir)  # make it here so we can start logging to logfile
            ex = Execution(id=old_id, name=name, output_dir=output_dir, manual_instantiation=False)
            session.add(ex)

        ex.max_cpus = max_cpus
        ex.max_attempts = max_attempts
        ex.info['last_cmd_executed'] = get_last_cmd_executed()
        ex.info['cwd'] = os.getcwd()
        session.commit()
        session.expunge_all()
        session.add(ex)

        ex.cosmos_app = self

        return ex


    def initdb(self):
        """
        Initialize the database via sql CREATE statements.  If the tables already exists, nothing will happen.
        """
        print >> sys.stderr, 'Initializing sql database for Cosmos v%s...' % __version__
        Base.metadata.create_all(bind=self.session.bind)
        from .db import MetaData

        meta = MetaData(initdb_library_version=__version__)
        self.session.add(meta)
        self.session.commit()

    def resetdb(self):
        """
        Resets (deletes then initializes) the database.  This is not reversible!
        """
        print >> sys.stderr, 'Dropping tables in db...'
        Base.metadata.drop_all(bind=self.session.bind)
        self.initdb()

    def shell(self):
        """
        Launch an IPython shell with useful variables already imported.
        """
        cosmos_app = self
        session = self.session
        executions = self.session.query(Execution).order_by('id').all()
        ex = executions[-1] if len(executions) else None

        import IPython

        IPython.embed()

    def runweb(self, host, port, debug=True):
        """
        Starts the web dashboard
        """
        return self.flask_app.run(debug=debug, host=host, port=port)


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


NOOP = '<NO OPERATION>'


class TaskStatus(MyEnum):
    no_attempt = 'Has not been attempted',
    waiting = 'Waiting to execute',
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


########################################################################################################################
# Imports
########################################################################################################################

from .models.TaskFile import TaskFile, abstract_output_taskfile_old, abstract_input_taskfile, abstract_output_taskfile
from .models.Task import Task
from .models.Stage import Stage
from .models.Tool import Tool, Tool_old, Input, Inputs
from .models.Execution import Execution
from .util.args import add_execution_args
from .util.tool import one2one, one2many, many2one, many2many, make_dict
from .graph.draw import draw_task_graph, draw_stage_graph