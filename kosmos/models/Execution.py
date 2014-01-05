from ..db import Base, session
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from .. import TaskStatus, Task, Stage
import os
opj = os.path.join
import signal
from ..util.helpers import get_logger, mkdir

class Execution(Base):
    __tablename__ = 'execution'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    output_dir = Column(String, nullable=False)
    created_on = Column(DateTime, default=func.now())
    finished_on = Column(DateTime)

    def __init__(self,*args, **kwargs):
        super(Execution, self).__init__(*args, **kwargs)

    def run(self, taskgraph, get_output_dir, get_log_dir,settings={}, parameters={},):
        mkdir(self.output_dir)
        self.log = get_logger('kosmos-%s'%Execution, opj(self.output_dir,'execution.log'))

        #terminate on ctrl+c
        try:
            def ctrl_c(signal, frame):
                self.terminate()
            signal.signal(signal.SIGINT, ctrl_c)
        except ValueError: #signal only works in parse_args thread and django complains
            pass

        from sqlalchemy import inspect
        session = inspect(self).session
        if session:
            print 'saving stages %s' % taskgraph.stages
            for stage in taskgraph.stages:
                #assert stage.execution is None or stage.execution is self, 'taskgraph already belongs to a different execution'
                stage.id = None
                stage.execution = self
                session.add(stage)
            for task in taskgraph.task_G.nodes():
                session.add(task)

            session.commit()

        from ..job.JobManager import JobManager
        job_manager = JobManager()
        task_queue = _copy_graph(taskgraph.task_G)
        successful = filter(lambda t: t.status == TaskStatus.successful, taskgraph.task_G.nodes())
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

        return self

    def terminate(self):
        raise

    @property
    def tasks(self):
        return session.query(Task).filter(Task.stage_id.in_(s.id for s in self.stages)).all()

    def __repr__(self):
        return '<Execution[%s] %s>' % (self.id or '',self.name)


def _copy_graph(graph):
    import networkx as nx
    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2

def _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters):
    ready_tasks = [task for task,degree in task_queue.in_degree().items() if degree == 0 and task.status == TaskStatus.no_attempt]
    for ready_task in ready_tasks:
        ready_task.configure(settings, parameters)
        ready_task.output_dir = get_output_dir(ready_task)

        ## render taskfile paths
        for f in ready_task.taskfiles:
            if f.path is None:
                f.path = os.path.join(ready_task.output_dir, f.basename)

        ready_task.log_dir = get_log_dir(ready_task)

        job_manager.submit(ready_task)
