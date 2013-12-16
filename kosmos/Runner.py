from JobManager import JobManager
from .signals import task_finished
import os

def copy_graph(graph):
    import networkx as nx
    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2


def all_nodes_finished(graph):
    return all(map(lambda s: s.is_finished, graph.nodes()))

class Runner(object):
    def __init__(self,taskgraph, get_output_dir, get_log_dir, settings={}, parameters={}):
        self.taskgraph = taskgraph
        self.job_manager = JobManager()
        self.settings = settings
        self.parameters = parameters
        self.get_output_dir = get_output_dir
        self.get_log_dir = get_log_dir


    def run_ready_tasks(self, taskgraph):
        task_queue = copy_graph(taskgraph.task_G)
        is_finished = filter(lambda t: t.is_finished, taskgraph.task_G.nodes())
        task_queue.remove_nodes_from(is_finished)

        for ready_task in map(lambda x: x[0], filter(lambda x: x[1] == 0, task_queue.in_degree().items())):
            ready_task.configure(self.settings, self.parameters)
            ready_task.output_dir = self.get_output_dir(ready_task)

            ## render taskfile paths
            for f in ready_task.output_files:
                if f.path is None:
                    f._path = os.path.join(ready_task.output_dir, f.basename)
            ##
            ready_task.log_dir = self.get_log_dir(ready_task)

            self.get_output_dir
            self.job_manager.submit(ready_task, )

    def run_ready_stages(self):
        for ready_stage in map(lambda x: x[0], filter(lambda x: x[1] == 0, self.stage_queue.in_degree().items())):
            print '%s is ready' % ready_stage
            self.taskgraph.resolve_stage(ready_stage)
            self.run_ready_tasks(self.taskgraph)
            self.stage_queue.remove_node(ready_stage)

    def run(self):
        self.stage_queue = copy_graph(self.taskgraph.stage_G)

        while True:
            self.run_ready_stages()
            # Wait for a task to finish
            task = self.job_manager.wait_for_a_job_to_finish()
            task_finished.send(task)
            task.is_finished = True
            print '%s finished' % task

            if len(self.stage_queue) == 0 and all_nodes_finished(self.taskgraph.task_G):
                break
