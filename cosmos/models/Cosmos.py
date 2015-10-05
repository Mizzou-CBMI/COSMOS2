from flask import Flask

import sys
import os
from ..util.helpers import get_logger, mkdir, confirm, str_format
import itertools as it
from ..util.args import get_last_cmd_executed
from ..db import Base
from .. import __version__
import math
from concurrent import futures

def default_get_submit_args(task, default_queue=None):
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
    use_time_req = False

    cpu_req = task.cpu_req
    mem_req = task.mem_req if use_mem_req else None
    time_req = task.time_req if use_time_req else None

    jobname = '%s_task(%s)' % (task.stage.name, task.id)
    queue = ' -q %s' % default_queue if default_queue else ''
    priority = ' -p %s' % default_job_priority if default_job_priority else ''

    if drm == 'lsf':
        rusage = '-R "rusage[mem={mem}] ' if mem_req and use_mem_req else ''
        time = ' -W 0:{0}'.format(task.time_req) if task.time_req else ''
        return '-R "{rusage}span[hosts=1]" -n {task.cpu_req}{time}{queue} -J "{jobname}"'.format(**locals())

    elif drm == 'ge':
        h_vmem = int(math.ceil(mem_req / float(cpu_req))) if mem_req else None

        def g():
            resource_reqs = dict(h_vmem=h_vmem, num_proc=cpu_req, time_req=time_req)
            for k, v in resource_reqs.items():
                if v is not None:
                    yield '%s=%s' % (k, v)

        resource_str = ','.join(g())

        return '-l "{resource_str}"{priority} -N "{jobname}"'.format(**locals())
    elif drm == 'local':
        return None
    else:
        raise Exception('DRM not supported: %s' % drm)


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

        self.futures_executor = futures.ThreadPoolExecutor(10)
        if flask_app:
            self.flask_app = flask_app
        else:
            self.flask_app = Flask(__name__)
            self.flask_app.secret_key = os.urandom(24)

        self.get_submit_args = get_submit_args
        self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = database_url
        # self.flask_app.config['SQLALCHEMY_ECHO'] = True

        from flask_sqlalchemy import SQLAlchemy

        self.sqla = SQLAlchemy(self.flask_app)
        self.session = self.sqla.session

        from sqlalchemy.pool import StaticPool, NullPool
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine(database_url)
        # connect_args={'check_same_thread':False},
        # poolclass=StaticPool)

        # from flask_sqlalchemy_session import flask_scoped_session, current_session
        # session_factory = sessionmaker(bind=engine)
        # self.Session = flask_scoped_session(session_factory, flask_app)

        self.default_queue = default_queue
        self.default_drm = default_drm

    def configure_flask(self):
        # setup flask views
        from cosmos.web.views import gen_bprint
        # from cosmos.web.admin import add_cosmos_admin

        self.cosmos_bprint = gen_bprint(self)
        self.flask_app.register_blueprint(self.cosmos_bprint)
        # add_cosmos_admin(flask_app, self.session)

    # @property
    # def session(self):
    #     return self.Session()

    def close(self):
        self.futures_executor.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def start(self, name, output_dir=os.getcwd(), restart=False, skip_confirm=False, max_cpus=None, max_attempts=1,
              check_output_dir=True):
        from .Execution import Execution

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
        from ..util.helpers import mkdir

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
                ex.log.info('Deleting %s unsuccessful task(s) from SQL database, delete_files=%s' % (n, False))
                for t in failed_tasks:
                    session.delete(t)

            for stage in it.ifilter(lambda s: len(s.tasks) == 0, ex.stages):
                ex.log.info('Deleting stage %s, since it has 0 successful Tasks' % stage)
                session.delete(stage)

        else:
            # start from scratch
            if check_output_dir:
                assert not os.path.exists(output_dir), 'Execution.output_dir `%s` already exists.' % (output_dir)

            ex = Execution(id=old_id, name=name, output_dir=output_dir, manual_instantiation=False)
            mkdir(output_dir)  # make it here so we can start logging to logfile
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
        from ..db import MetaData

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
        from .Execution import Execution

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
        self.configure_flask()
        return self.flask_app.run(debug=debug, host=host, port=port)
