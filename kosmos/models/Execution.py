from ..db import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
import os
from ..signals import task_finished


class Execution(Base):
    __tablename__ = 'execution'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    created_on = Column(DateTime, default=func.now())
    finished_on = Column(DateTime, default=func.now())


    def run(self, taskgraph, get_output_dir, get_log_dir, session = None, settings={}, parameters={},):
        if session:
            print 'saving stages %s' % taskgraph.stages
            for stage in taskgraph.stages:
                session.add(stage)
            session.commit()
            print 'saved'

        from ..jobmanager.JobManager import JobManager
        job_manager = JobManager()
        task_queue = _copy_graph(taskgraph.task_G)
        is_finished = filter(lambda t: t.is_finished, taskgraph.task_G.nodes())
        task_queue.remove_nodes_from(is_finished)

        while True:
            _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters)
            # Wait for a task to finish
            task = job_manager.wait_for_a_job_to_finish()
            if task is not None:
                task_finished.send(task)
                print '%s finished' % task

            if _all_nodes_finished(taskgraph.task_G):
                break

        return self


def _copy_graph(graph):
    import networkx as nx
    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2


def _all_nodes_finished(graph):
    return all(map(lambda t: t.is_finished, graph.nodes()))

def _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters):
    ready_tasks = map(lambda x: x[0], filter(lambda x: x[1] == 0, task_queue.in_degree().items()))
    for ready_task in ready_tasks:
        ready_task.configure(settings, parameters)
        ready_task.output_dir = get_output_dir(ready_task)

        ## render taskfile paths
        for f in ready_task.output_files:
            if f.path is None:
                f._path = os.path.join(ready_task.output_dir, f.basename)

        ready_task.log_dir = get_log_dir(ready_task)

        job_manager.submit(ready_task)
        task_queue.remove_node(ready_task)
