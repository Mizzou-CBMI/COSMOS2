import re

from ..helpers import groupby
import networkx as nx

class TaskGraph(object):
    """
    A Representation of a workflow as a :class:`TaskGraph` of jobs.
    """

    def __init__(self, recipe=None):
        self.task_G = nx.DiGraph()
        self.stages = []
        if recipe:
            self.resolve(recipe)

    def resolve(self,recipe):
        for stage in nx.topological_sort(recipe.stage_G):
            self.stages.append(stage)
            self._resolve_stage(stage)

    def _resolve_stage(self, stage):
        if not stage.resolved:
            if stage.is_source:
                #stage.tasks is already set
                for task in stage.tasks:
                    self._add_task_to_task_G(task)
            else:
                for new_task, paren_tasks in stage.rel.__class__.gen_tasks(stage):
                    new_task.dag = self
                    stage.tasks.append(new_task)
                    self._add_task_to_task_G(new_task, paren_tasks)
        stage.resolved = True

        ### Validation
        for task in self.task_G:
            for key in task.tags:
                if not re.match('\w', key):
                    raise ValueError("{0}.{1}'s tag's keys are not alphanumeric: {3}".format(stage, task, task.tags))

    def _add_task_to_task_G(self, new_task, parents=None):
        if parents is None:
            parents = []
        assert new_task.tags not in [t.tags for t in self.task_G.nodes() if
                                     t.stage == new_task.stage], 'Duplicate set of tags detected in {0}'.format(
            new_task.stage)

        self.task_G.add_node(new_task)
        for p in parents:
            self.task_G.add_edge(p, new_task)

        return self

    def save_image(self, save_to=None):
        """
        Writes the :class:`TaskGraph` as an image.
        gat
        :param path: the path to write to
        """
        import pygraphviz as pgv

        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname'] = "Courier"
        dag.node_attr['fontsize'] = 8
        dag.edge_attr['fontcolor'] = '#586e75'
        dag.graph_attr['bgcolor'] = '#fdf6e3'

        dag.add_nodes_from(self.task_G.nodes())
        dag.add_edges_from(self.task_G.edges())
        for stage, tasks in groupby(self.task_G.nodes(), lambda x: x.stage):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage), label=stage.label, color='lightgrey')

        dag.layout(prog="dot")
        return dag.draw(path=save_to, format='svg')

class DAGError(Exception): pass
class StageNameCollision(Exception): pass
class FlowFxnValidationError(Exception): pass
