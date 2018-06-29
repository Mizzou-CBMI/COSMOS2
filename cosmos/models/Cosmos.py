import os
import sys
# from concurrent import futures
from datetime import datetime

from flask import Flask

from cosmos import WorkflowStatus
from cosmos import __version__
from cosmos.db import Base
from cosmos.models import Task
from cosmos.util.args import get_last_cmd_executed
from cosmos.util.helpers import confirm


def default_get_submit_args(task, parallel_env='orte'):
    """
    Default method for determining the extra arguments to pass to the DRM.
    For example, returning `"-n 3" if` `task.drm == "lsf"` would cause all jobs
    to be submitted with `bsub -n 3`.

    :param cosmos.api.Task task: The Task being submitted.
    :rtype: str
    """
    default_job_priority = None
    use_mem_req = False

    jobname = '%s[%s]' % (task.stage.name, task.uid.replace('/', '_'))

    if task.drm in ['lsf', 'drmaa:lsf']:
        rusage = '-R "rusage[mem={mem}] ' if task.mem_req and use_mem_req else ''
        time = ' -W 0:{0}'.format(task.time_req) if task.time_req else ''
        return '-R "{rusage}span[hosts=1]" -n {task.core_req}{time}{queue} -J "{jobname}"'.format(**locals())
    elif task.drm in ['ge', 'drmaa:ge']:
        return '-cwd -pe {parallel_env} {core_req}{priority} -N "{jobname}"{job_class}{queue}'.format(
            priority=' -p %s' % default_job_priority if default_job_priority else '',
            job_class=' -jc %s' % task.job_class if task.job_class else '',
            queue=' -q %s' % task.queue if task.queue else '',
            jobname=jobname,
            core_req=task.core_req,
            parallel_env=parallel_env)
    elif task.drm == 'slurm':
        return '-c {cores} {partition}{jobname_str}{mem_str}{time_str}'.format(
            mem_str=(' --mem %s' % task.mem_req) if task.mem_req is not None else '',
            time_str=(' --time %s' % task.time_req) if task.time_req is not None else '',
            partition=' -p %s' % task.queue if task.queue else '',
            jobname_str=' -J %s' % jobname if jobname else '',
            cores=task.core_req,
            jobname=jobname)
    else:
        return None


class Cosmos(object):
    def __init__(self,
                 database_url='sqlite:///:memory:',
                 get_submit_args=default_get_submit_args,
                 default_drm='local',
                 default_drm_options=None,
                 default_queue=None,
                 default_time_req=None,
                 default_max_attempts=1,
                 flask_app=None,
                 default_job_class=None):
        """
        :param str database_url: A `sqlalchemy database url <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_.  ex: sqlite:///home/user/sqlite.db or
            mysql://user:pass@localhost/database_name or postgresql+psycopg2://user:pass@localhost/database_name
        :param callable get_submit_args: a function that returns arguments to be passed to the job submitter, like resource
            requirements or the queue to submit to.  See :func:`cosmos.api.default_get_submit_args` for details
        :param flask.Flask flask_app: A Flask application instance for the web interface.  The default behavior is to create one.
        :param str default_drm: The Default DRM to use (ex 'local', 'lsf', or 'ge')
        """
        default_drm_options = {} if default_drm_options is None else default_drm_options
        # Avoid cyclical import dependencies
        from cosmos.job.drm.DRM_Base import DRM

        assert default_drm.split(':')[0] in DRM.get_drm_names(), 'unsupported drm: %s' % \
                                                                                      default_drm.split(':')[0]
        assert '://' in database_url, 'Invalid database_url: %s' % database_url

        # self.futures_executor = futures.ThreadPoolExecutor(10)
        if flask_app:
            self.flask_app = flask_app
        else:
            self.flask_app = Flask(__name__)
            self.flask_app.secret_key = os.urandom(24)

        self.get_submit_args = get_submit_args
        # self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        # self.flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.flask_app.jinja_env.globals['time_now'] = datetime.now()
        # self.flask_app.config['SQLALCHEMY_ECHO'] = True

        # from flask_sqlalchemy import SQLAlchemy
        #
        #
        # self.sqla = SQLAlchemy(self.flask_app)
        # self.session = self.sqla.session

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker, scoped_session
        from sqlalchemy.ext.declarative import declarative_base

        engine = create_engine(database_url, convert_unicode=True)
        self.session = scoped_session(sessionmaker(autocommit=False,
                                                   autoflush=False,
                                                   bind=engine))

        Base = declarative_base()
        Base.query = self.session.query_property()

        @self.flask_app.teardown_appcontext
        def shutdown_session(exception=None):
            self.session.remove()

        self.default_drm = default_drm
        self.default_drm_options = default_drm_options
        self.default_job_class = default_job_class
        self.default_queue = default_queue
        self.default_max_attempts = default_max_attempts
        self.default_time_req = default_time_req

        # def configure_flask(self):
        # setup flask views
        # from cosmos.web.admin import add_cosmos_admin

        # add_cosmos_admin(flask_app, self.session)

    # @property
    # def session(self):
    #     return self.Session()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def start(self, name, restart=False, skip_confirm=False, primary_log_path='workflow.log', fail_fast=False):
        """
        Start, resume, or restart an workflow based on its name.  If resuming, deletes failed tasks.

        :param str name: A name for the workflow.  Must be unique for this Cosmos session.
        :param bool restart: If True and the workflow exists, delete it first.
        :param bool skip_confirm: (If True, do not prompt the shell for input before deleting workflows or files.
        :param str primary_log_path: The path of the primary log to write to.  If None, does not write to a file.  Log information is always printed to stderr.
        :param bool fail_fast: If True, terminate the workflow the first time a Task fails.
        :param int default_max_attempts: The default maximum number of times to attempt a Task.

        Otherwise, run all Tasks except those downstream of a failure.
        :rtype Workflow:
        :returns: An Workflow instance.
        """
        from .Workflow import Workflow
        assert os.path.exists(
            os.getcwd()), "The current working dir of this environment, %s, does not exist" % os.getcwd()
        # output_dir = os.path.abspath(output_dir)
        # output_dir = output_dir if output_dir[-1] != '/' else output_dir[0:]  # remove trailing slash
        # prefix_dir = os.path.split(output_dir)[0]
        # assert os.path.exists(prefix_dir), '%s does not exist' % prefix_dir
        from ..util.helpers import mkdir

        # assert isinstance(primary_log_path, basestring) and len(primary_log_path) > 0, 'invalid parimary log path'
        if primary_log_path is not None and os.path.dirname(primary_log_path):
            mkdir(os.path.dirname(primary_log_path))

        session = self.session

        old_id = None
        if restart:
            wf = session.query(Workflow).filter_by(name=name).first()
            if wf:
                old_id = wf.id
                msg = 'Restarting %s.  Are you sure you want to delete the all sql records?' % wf
                if not skip_confirm and not confirm(msg):
                    raise SystemExit('Quitting')

                wf.delete(delete_files=False)
            else:
                if not skip_confirm and not confirm('Workflow with name %s does not exist, '
                                                    'but `restart` is set to True.  '
                                                    'Continue by starting a new Workflow?' % name):
                    raise SystemExit('Quitting')

        # resuming?
        wf = session.query(Workflow).filter_by(name=name).first()
        # msg = 'Workflow started, Cosmos v%s' % __version__
        if wf:
            # resuming.
            if not skip_confirm and not confirm('Resuming %s.  All non-successful jobs will be deleted, '
                                                'then any new tasks in the graph will be added and executed.  '
                                                'Are you sure?' % wf):
                raise SystemExit('Quitting')
            # assert ex.cwd == output_dir, 'cannot change the output_dir of an workflow being resumed.'

            wf.successful = False
            wf.finished_on = None
            wf.status = WorkflowStatus.resuming

            # if not os.path.exists(wf.output_dir):
            #     raise IOError('output_directory %s does not exist, cannot resume %s' % (wf.output_dir, wf))

            wf.log.info('Resuming %s' % wf)
            session.add(wf)
            failed_tasks = [t for s in wf.stages for t in s.tasks if not t.successful]
            n = len(failed_tasks)
            if n:
                wf.log.info('Deleting %s unsuccessful task(s) from SQL database, delete_files=%s' % (n, False))
                for t in failed_tasks:
                    session.delete(t)

            for stage in filter(lambda s: len(s.tasks) == 0, wf.stages):
                wf.log.info('Deleting stage %s, since it has 0 successful Tasks' % stage)
                session.delete(stage)

        else:
            # start from scratch
            # if check_output_dir:
            #     assert not os.path.exists(output_dir), 'Workflow.output_dir `%s` already exists.' % (output_dir)

            wf = Workflow(id=old_id, name=name, manual_instantiation=False, successful=False)
            # mkdir(output_dir)  # make it here so we can start logging to logfile
            session.add(wf)

        wf.info['last_cmd_executed'] = get_last_cmd_executed()
        wf.info['cwd'] = os.getcwd()
        wf.info['fail_fast'] = fail_fast
        wf.primary_log_path = primary_log_path

        wf.log.info('Committing SQL session...')
        session.commit()
        session.expunge_all()
        session.add(wf)
        wf.log.info('Execution Command: %s' % get_last_cmd_executed())

        wf.cosmos_app = self

        return wf

    def initdb(self):
        """
        Initialize the database via sql CREATE statements.  If the tables already exists, nothing will happen.
        """
        print >> sys.stderr, 'Initializing sql database for Cosmos v%s...' % __version__
        Base.metadata.create_all(bind=self.session.bind)
        from ..db import MetaData

        meta = MetaData(initdb_library_version=__version__)
        self.session.add(meta)
        self.session.commit()
        return self

    def resetdb(self):
        """
        Resets (deletes then initializes) the database.  This is not reversible!
        """
        print >> sys.stderr, 'Dropping tables in db...'
        Base.metadata.drop_all(bind=self.session.bind)
        self.initdb()
        return self

    def shell(self):
        """
        Launch an IPython shell with useful variables already imported.
        """
        from .Workflow import Workflow

        cosmos_app = self
        session = self.session
        workflows = self.session.query(Workflow).order_by('id').all()
        wf = workflows[-1] if len(workflows) else None

        import IPython

        IPython.embed()

    def init_flask(self):
        from cosmos.web.views import gen_bprint
        self.cosmos_bprint = gen_bprint(self.session)
        self.flask_app.register_blueprint(self.cosmos_bprint)
        return self.flask_app

    def runweb(self, host, port, debug=True):
        """
        Starts the web dashboard
        :param str host: Host name to bind to.  Default is local host, but commonly 0.0.0.0 to allow outside internet traffic.
        :param int port: Port to bind to.
        """
        self.init_flask()
        return self.flask_app.run(debug=debug, host=host, port=port)
