import re

import networkx as nx
from ..util.helpers import groupby
from .. import TaskStatus


class TaskGraph(object):
    """
    A Representation of a workflow as a :class:`TaskGraph` of jobs.
    """

    def __init__(self, recipe=None):
        self.task_G = nx.DiGraph()
        self.stages = []
        if recipe:
            self.resolve(recipe)

    def add_tasks(self, tasks):
        self.task_G.add_nodes_from(tasks)
        self.task_G.add_edges_from([ (parent, task) for task in tasks for parent in task.parents ])

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
            new_task.parents=parents
            self.task_G.add_edge(p, new_task)

        return self


    def createAGraph(self):
        import pygraphviz as pgv
        agraph = pgv.AGraph(strict=False, directed=True, fontname="Courier")
        agraph.node_attr['fontname'] = "Courier"
        agraph.node_attr['fontcolor'] = '#000'
        agraph.node_attr['fontsize'] = 8
        agraph.graph_attr['fontsize'] = 8
        agraph.edge_attr['fontcolor'] = '#586e75'
        #dag.graph_attr['bgcolor'] = '#fdf6e3'

        agraph.add_edges_from(self.task_G.edges())
        for stage, tasks in groupby(self.task_G.nodes(), lambda x: x.stage):
            sg = agraph.add_subgraph(name="cluster_{0}".format(stage), label=str(stage), color='grey', style='dotted')
            for task in tasks:
                def truncate_val(kv):
                    v = "{0}".format(kv[1])
                    v = v if len(v) < 10 else v[1:8] + '..'
                    return "{0}: {1}".format(kv[0], v)

                label = " \\n".join(map(truncate_val, task.tags.items()))
                status2color = {TaskStatus.no_attempt: 'black',
                                TaskStatus.waiting: 'gold1',
                                TaskStatus.successful: 'darkgreen',
                                TaskStatus.failed: 'darkred'}

                sg.add_node(task, label=label, URL=task.url, target="_blank",
                            color=status2color[task.status])

        return agraph

    def as_image(self, path=None):
        g = self.createAGraph()
        g.layout(prog="dot")
        return g.draw(path=path, format='svg')

class DAGError(Exception): pass
class StageNameCollision(Exception): pass
class FlowFxnValidationError(Exception): pass
