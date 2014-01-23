from ..db import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, event, orm, PickleType
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import validates, synonym
from flask import url_for
import os, re
import shutil
opj = os.path.join
import signal

from .. import taskgraph
from ..job.JobManager import JobManager
from .. import TaskStatus, Task, __version__, StageStatus, ExecutionStatus, signal_execution_status_change
from .Recipe import recipe_image

from ..util.helpers import get_logger, mkdir, confirm
from ..util.sqla import Enum34_ColumnType, MutableDict, JSONEncodedDict


def default_task_log_output_dir(task):
    return opj(task.execution.output_dir, 'log', task.stage.name, str(task.id))


def default_task_output_dir(task):
    return opj(task.execution.output_dir, task.stage.name, str(task.id))

@signal_execution_status_change.connect
def execution_status_changed(ex):
    ex.log.info('%s %s, output_dir: %s' % (ex, ex.status, ex.output_dir))

    if ex.status in [ExecutionStatus.successful, ExecutionStatus.failed, ExecutionStatus.killed]:
        ex.finished_on = func.now()

    if ex.status == ExecutionStatus.successful:
        ex.successful = True

    ex.session.commit()


class Execution(Base):
    __tablename__ = 'execution'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)
    successful = Column(Boolean, nullable=False, default=False)
    output_dir = Column(String, nullable=False)
    created_on = Column(DateTime, default=func.now())
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    recipe_graph = Column(PickleType)
    max_cpus = Column(Integer)
    info = Column(MutableDict.as_mutable(JSONEncodedDict))
    _status = Column(Enum34_ColumnType(ExecutionStatus), default=ExecutionStatus.no_attempt)

    exclude_from_dict = ['recipe_graph']

    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            if self._status != value:
                self._status = value
                signal_execution_status_change.send(self)

        return synonym('_status', descriptor=property(get_status, set_status))


    @validates('name')
    def validate_name(self, key, name):
        assert re.match('^[\w]+$', name), 'Invalid execution name.'
        return name

    @classmethod
    def start(cls, session, name, output_dir, restart=False, prompt_confirm=True, max_cpus=None):
        #assert name is not None, 'name cannot be None'
        assert output_dir is not None, 'output_dir cannot be None'

        if restart:
            ex = session.query(Execution).filter_by(name=name).first()
            if ex:
                msg = 'Are you sure you want to `rm -rf %s` and delete all sql records of %s?' % (ex.output_dir, ex)
                if prompt_confirm and not confirm(msg):
                    raise SystemExit('Quitting')

                ex.delete(delete_output_dir=True)

        #resuming?
        ex = session.query(Execution).filter_by(name=name).first()
        msg = 'Execution started, Kosmos v%s' % __version__
        if ex:
            #resuming.
            ex.max_cpus = max_cpus
            assert ex.output_dir == output_dir, 'cannot change the output_dir of a workflow being resumed.'

            ex.log.info(msg)
            session.add(ex)
            q = ex.tasksq.filter_by(successful=False)
            n = q.count()
            if n:
                ex.log.info('Deleting %s failed tasks' % n)
                for t in q.all():
                    session.delete(t)
        else:
            #start from scratch
            assert not os.path.exists(output_dir), '%s already exists'.format(output_dir)
            mkdir(output_dir)
            ex = Execution(name=name, output_dir=output_dir, max_cpus=max_cpus)
            ex.log.info(msg)
            session.add(ex)

        session.commit()
        return ex

    @orm.reconstructor
    def constructor(self):
        self.__init__()

    def __init__(self, *args, **kwargs):
        super(Execution, self).__init__(*args, **kwargs)
        assert self.output_dir is not None, 'output_dir cannot be None'
        mkdir(self.output_dir)
        self.log = get_logger('kosmos-%s' % Execution, opj(self.output_dir, 'execution.log'))
        if self.info is None:
            self.info = dict()
        self.jobmanager = None

    def run(self, recipe, task_output_dir=default_task_output_dir, task_log_output_dir=default_task_log_output_dir,
            settings={},
            parameters={},
            dry=False):
        try:
            session = self.session
            assert session, 'Execution must be part of a sqlalchemy session'
            self.jobmanager = JobManager()
            if self.started_on is None:
                self.started_on = func.now()

            # Render task graph and save to db
            task_g, stage_g = taskgraph.render_recipe(self, recipe)
            self.recipe_graph = recipe_image(stage_g)
            session.add_all(stage_g.nodes())
            session.add_all(task_g.nodes())

            # Create Task Queue
            task_queue = _copy_graph(task_g)
            successful = filter(lambda t: t.status == TaskStatus.successful, task_g.nodes())
            self.log.info('Skipping %s successful tasks' % len(successful))
            task_queue.remove_nodes_from(successful)
            self.log.info('Queueing %s new tasks' % len(task_queue.nodes()))

            terminate_on_ctrl_c(self)

            session.commit()  # required to set IDs for some of the output_dir generation functions
            # Set output_dirs of task_queue tasks
            log_dirs = {t.log_dir: t for t in successful}
            for task in task_queue.nodes():
                task.output_dir = task_output_dir(task)
                log_dir = task_log_output_dir(task)
                assert log_dir not in log_dirs, 'Duplicate log_dir detected for %s and %s' % (task, log_dirs[log_dir])
                log_dirs[log_dir] = task
                task.log_dir = log_dir

            session.commit()
            if not dry:
                while len(task_queue) > 0:
                    _run_ready_tasks(task_queue, settings, parameters, self)
                    for task in _process_finished_tasks(self.jobmanager):
                        task_queue.remove_node(task)

                self.status = ExecutionStatus.successful
            session.commit()
            return self

        except Exception as e:
            self.log.error(e)
            session.commit()
            raise

    def terminate(self):
        if self.jobmanager:
            self.log.info('Processing finished tasks')
            _process_finished_tasks(self.jobmanager, at_least_one=False)
        self.status = ExecutionStatus.killed

    @property
    def tasksq(self):
        return self.session.query(Task).filter(Task.stage_id.in_(s.id for s in self.stages))

    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]

    @property
    def url(self):
        return url_for('kosmos.execution', id=self.id)

    def __repr__(self):
        return '<Execution[%s] %s>' % (self.id or '', self.name)

    def delete(self, delete_output_dir=False):
        if delete_output_dir:
            self.log.info('Deleting %s' % self.output_dir)
            shutil.rmtree(self.output_dir)
        self.session.delete(self)
        self.session.commit()


@event.listens_for(Execution, 'before_delete')
def before_delete(mapper, connection, target):
    print 'before_delete %s ' % target


def _copy_graph(graph):
    import networkx as nx

    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2


def _run_ready_tasks(task_queue, settings, parameters, execution):
    max_cpus = execution.max_cpus
    ready_tasks = [task for task, degree in task_queue.in_degree().items() if
                   degree == 0 and task.status == TaskStatus.no_attempt]
    for ready_task in sorted(ready_tasks, key=lambda t: t.cpu_req):
        cores_used = sum([t.cpu_req for t in execution.jobmanager.running_tasks])
        if max_cpus is not None and ready_task.cpu_req + cores_used > max_cpus:
            execution.log.info('Reached max_cpus limit of %s, waiting for a task to finish...' % max_cpus)
            break

        ready_task.configure(settings, parameters)

        ## render taskfile paths
        for f in ready_task.taskfiles:
            if f.path is None:
                f.path = os.path.join(ready_task.output_dir, f.basename)

        execution.jobmanager.submit(ready_task)


def _process_finished_tasks(jobmanager, at_least_one=True):
    for task in jobmanager.get_finished_tasks(at_least_one=at_least_one):
        if task.profile.get('exit_status', None) == 0 or task.NOOP:
            task.status = TaskStatus.successful
            yield task
        else:
            if not task.must_succeed:
                yield task
            task.status = TaskStatus.failed


def terminate_on_ctrl_c(execution):
#terminate on ctrl+c
    try:
        def ctrl_c(signal, frame):
            execution.log.info('Caught SIGINT (ctrl+c)')
            execution.terminate()
            raise SystemExit('Execution terminated with a SIGINT (ctrl+c) event')

        signal.signal(signal.SIGINT, ctrl_c)
    except ValueError: #signal only works in parse_args thread and django complains
        pass