from ..db import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, event, orm
from sqlalchemy import inspect
from .. import TaskStatus, Task, Stage, taskgraph, Recipe
import os

opj = os.path.join
import signal
from ..util.helpers import get_logger, mkdir
from ..util.sqla import get_or_create
from .. import rel


class Execution(Base):
    __tablename__ = 'execution'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    output_dir = Column(String, nullable=False)
    created_on = Column(DateTime, default=func.now())
    finished_on = Column(DateTime)

    @classmethod
    def start(cls, session, name, output_dir):
        ex = session.query(Execution).filter_by(name=name).first()
        if ex:
            session.add(ex)
            q = ex.tasksq.filter_by(successful=False)
            n = q.count()
            if n:
                ex.log.info('Deleting %s failed tasks' % n)
                q.delete('fetch')
            return ex
        else:
            ex = Execution(name=name, output_dir=output_dir)
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

    # def add_source(self, tasks, name=None):
    #     self.recipe.add_source( tasks, name=None)
    #
    # def add_stage(self, task_class, parents, rel=rel.One2one, name=None, extra_tags=None):
    #     self.recipe.add_stage(task_class, parents, rel=rel.One2one, name=None, extra_tags=None)

    def run(self, recipe, get_output_dir, get_log_dir, settings={}, parameters={}, ):
        session = inspect(self).session
        assert session, 'Execution must be part of a sqlalchemy session'
        terminate_on_ctrl_c(self)

        task_g, stage_g = taskgraph.render_recipe(self, recipe)
        session.add_all([t for t in task_g.nodes() ])
        session.commit()

        from ..job.JobManager import JobManager

        job_manager = JobManager()
        task_queue = _copy_graph(task_g)
        successful = filter(lambda t: t.status == TaskStatus.successful, task_g.nodes())
        self.log.info('Skipping %s successful tasks'%len(successful))
        task_queue.remove_nodes_from(successful)

        while len(task_queue) > 0:
            _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters)
            # Wait for a task to finish
            task = job_manager.wait_for_a_job_to_finish()
            if task is not None:
                if task.profile.get('exit_status', None) == 0:
                    task.status = TaskStatus.successful
                    task_queue.remove_node(task)
                else:
                    task.status = TaskStatus.failed

                    # if all(t.finished for t in taskgraph.task_G.nodes()):
                    #     break

        self.log.info('Execution successful')
        return self

    def terminate(self):
        raise

    @property
    def tasksq(self):
        return inspect(self).session.query(Task).filter(Task.stage_id.in_(s.id for s in self.stages))

    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]

    def __repr__(self):
        return '<Execution[%s] %s>' % (self.id or '', self.name)


@event.listens_for(Execution, 'before_delete')
def before_delete(mapper, connection, target):
    print 'before_delete %s ' % target


def _copy_graph(graph):
    import networkx as nx

    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2


def _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters):
    ready_tasks = [task for task, degree in task_queue.in_degree().items() if
                   degree == 0 and task.status == TaskStatus.no_attempt]
    for ready_task in ready_tasks:
        ready_task.configure(settings, parameters)
        ready_task.output_dir = get_output_dir(ready_task)

        ## render taskfile paths
        for f in ready_task.taskfiles:
            if f.path is None:
                f.path = os.path.join(ready_task.output_dir, f.basename)

        ready_task.log_dir = get_log_dir(ready_task)

        job_manager.submit(ready_task)


def terminate_on_ctrl_c(execution):
#terminate on ctrl+c
    try:
        def ctrl_c(signal, frame):
            execution.terminate()

        signal.signal(signal.SIGINT, ctrl_c)
    except ValueError: #signal only works in parse_args thread and django complains
        pass