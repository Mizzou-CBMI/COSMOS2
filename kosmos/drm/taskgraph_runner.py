import os

import networkx as nx

from .JobManager import JobManager
from ..signals import task_finished


def _copy_graph(graph):
    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2


def _all_nodes_finished(graph):
    return all(map(lambda s: s.is_finished, graph.nodes()))

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

def run(taskgraph, get_output_dir, get_log_dir, settings={}, parameters={}):
    job_manager = JobManager()
    taskgraph.task_G = nx.DiGraph()

    for stage in nx.topological_sort(taskgraph.stage_G):
        print '%s is ready' % stage
        taskgraph._resolve_stage(stage)

    task_queue = _copy_graph(taskgraph.task_G)
    is_finished = filter(lambda t: t.is_finished, taskgraph.task_G.nodes())
    task_queue.remove_nodes_from(is_finished)

    while True:
        _run_ready_tasks(task_queue, job_manager, get_output_dir, get_log_dir, settings, parameters)
        # Wait for a task to finish
        task = job_manager.wait_for_a_job_to_finish()
        task_finished.send(task)
        print '%s finished' % task

        if _all_nodes_finished(taskgraph.task_G):
            break
