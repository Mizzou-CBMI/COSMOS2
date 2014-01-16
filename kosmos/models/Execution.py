from ..db import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, event, orm, PickleType
from Recipe import recipe_image
from .. import TaskStatus, Task, __version__
import os
from .. import taskgraph
from ..job.JobManager import JobManager
from flask import url_for

opj = os.path.join
import signal
from ..util.helpers import get_logger, mkdir


def default_task_log_output_dir(task):
    return opj(task.execution.output_dir, 'log', task.stage.name, task.tags_as_query_string())
def default_task_output_dir(task):
    return opj(task.execution.output_dir, task.stage.name, task.tags_as_query_string())


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
    info = Column(PickleType)

    exclude_from_dict = ['recipe_graph']

    @classmethod
    def start(cls, session, name, output_dir, restart=False):
        assert output_dir is not None, 'output_dir cannot be None'
        assert name is not None, 'name cannot be None'
        msg = 'Execution started, Kosmos v%s' % __version__

        if restart:
            ex = session.query(Execution).filter_by(name=name).first()
            if ex:
                ex.delete()

        ex = session.query(Execution).filter_by(name=name).first()
        if ex:
            assert ex.output_dir == output_dir, 'cannot change the output_dir of a workflow being resumed.'

            #resume execution
            ex.log.info(msg)
            session.add(ex)
            q = ex.tasksq.filter_by(successful=False)
            n = q.count()
            if n:
                ex.log.info('Deleting %s failed tasks' % n)
                for t in q.all():
                    session.delete(t)
        else:
            ex = Execution(name=name, output_dir=output_dir)
            ex.log.info(msg)
            session.add(ex)

        session.commit()
        return ex

    @orm.reconstructor
    def constructor(self):
        self.__init__()

    def __init__(self, *args, **kwargs):
        super(Execution, self).__init__(*args, **kwargs)
        mkdir(self.output_dir)
        self.log = get_logger('kosmos-%s' % Execution, opj(self.output_dir, 'execution.log'))
        self.info = {}

    def run(self, recipe, task_output_dir=default_task_output_dir, task_log_output_dir=default_task_log_output_dir, settings={},
            parameters={}, ):
        try:
            session = self.session
            assert session, 'Execution must be part of a sqlalchemy session'
            jobmanager = JobManager()
            if self.started_on is None:
                self.started_on = func.now()

            # Render task graph and save to db
            task_g, stage_g = taskgraph.render_recipe(self, recipe)
            self.recipe_graph = recipe_image(stage_g)
            session.add_all([t for t in task_g.nodes()])

            # Create Task Queue
            task_queue = _copy_graph(task_g)
            successful = filter(lambda t: t.status == TaskStatus.successful, task_g.nodes())
            self.log.info('Skipping %s successful tasks' % len(successful))
            task_queue.remove_nodes_from(successful)
            self.log.info('Queueing %s tasks' % len(task_queue.nodes()))

            terminate_on_ctrl_c(self, jobmanager, task_queue)

            # Set output_dirs of task_queue tasks
            log_dirs = {t.log_dir: t for t in successful}
            for task in task_queue.nodes():
                task.output_dir = task_output_dir(task)
                log_dir = task_log_output_dir(task)
                assert log_dir not in log_dirs, 'Duplicate log_dir detected for %s and %s' % (task, log_dirs[log_dir])
                log_dirs[log_dir] = task
                task.log_dir = log_dir

            session.commit()

            while len(task_queue) > 0:
                _run_ready_tasks(task_queue, jobmanager, settings, parameters)
                _process_finished_tasks(jobmanager, task_queue)

                # Wait for a task to finish

            self.log.info('Execution successful')
            self.finished_on = func.now()
            self.successful = True
            session.commit()
            return self
        except Exception as e:
            self.log.error(e)
            session.commit()
            raise

    def terminate(self):
        self.log.info('Terminating..')
        self.finished_on = func.now()
        self.session.commit()

    @property
    def tasksq(self):
        return self.session.query(Task).filter(Task.stage_id.in_(s.id for s in self.stages))

    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]

    @property
    def url(self):
        return url_for('.execution', id=self.id)

    def __repr__(self):
        return '<Execution[%s] %s>' % (self.id or '', self.name)

    def delete(self):
        self.session.delete(self)
        import shutil

        #shutil.rmtree(self.output_dir)
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


def _run_ready_tasks(task_queue, job_manager, settings, parameters):
    ready_tasks = [task for task, degree in task_queue.in_degree().items() if
                   degree == 0 and task.status == TaskStatus.no_attempt]
    for ready_task in ready_tasks:
        ready_task.configure(settings, parameters)

        ## render taskfile paths
        for f in ready_task.taskfiles:
            if f.path is None:
                f.path = os.path.join(ready_task.output_dir, f.basename)

        job_manager.submit(ready_task)


def _process_finished_tasks(jobmanager, task_queue, at_least_one=True):
    for task in jobmanager.get_finished_tasks(at_least_one=at_least_one):
        if task.profile.get('exit_status', None) == 0 or task.NOOP:
            task.status = TaskStatus.successful
            task_queue.remove_node(task)
        else:
            if not task.must_succeed:
                task_queue.remove_node(task)
            task.status = TaskStatus.failed


def terminate_on_ctrl_c(execution, jobmanager, task_queue):
#terminate on ctrl+c
    try:
        def ctrl_c(signal, frame):
            execution.log.info('Caught SIGINT (ctrl+c)')
            execution.log.info('Processing finished tasks')
            _process_finished_tasks(jobmanager, task_queue, at_least_one=False)
            jobmanager.terminate()
            execution.terminate()
            raise SystemExit, 'Workflow terminated with a SIGINT (ctrl+c) event'

        signal.signal(signal.SIGINT, ctrl_c)
    except ValueError: #signal only works in parse_args thread and django complains
        pass