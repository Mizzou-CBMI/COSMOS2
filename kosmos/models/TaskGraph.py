import re
import networkx as nx

from ..helpers import groupby
from ..models.Task import INPUT
from ..rel import one2many, many2one, one2one
from kosmos.models import Task


class TaskGraph(object):
    """
    A Representation of a workflow as a :class:`TaskGraph` of jobs.
    """

    def __init__(self):
        self.task_G = nx.DiGraph()
        self.stage_G = nx.DiGraph()

    def source(self, tasks, name=None):
        assert isinstance(tasks, list), 'tasks must be a list'
        assert len(tasks) > 0, '`tasks` cannot be empty'
        if name is None:
            name = tasks[0].name
        tags = [tuple(t.tags.items()) for t in tasks]
        assert len(tags) == len(
            set(tags)), 'Duplicate inputs tags detected for {0}.  Tags within a stage must be unique.'.format(INPUT)

        stage = Stage(task=type(tasks[0]), tasks=tasks, parents=[], rel=None, name=name, is_source=True)
        for task in stage.tasks:
            task.stage = stage

        self.stage_G.add_node(stage)

        return stage


    def stage(self, task, parents, rel=one2one, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        if name is None:
            if hasattr(task, 'name'):
                name = task.name
            else:
                name = task.__name__
        stage = Stage(name, task, parents, rel, extra_tags)

        assert stage.name not in [n.name for n in self.stage_G.nodes()], 'Duplicate stage names detected: {0}'.format(
            stage.name)

        self.stage_G.add_node(stage)
        for parent in stage.parents:
            self.stage_G.add_edge(parent, stage)

        return stage

    def _add_task_to_task_G(self, new_task, parents=None):
        if parents is None:
            parents = []
        assert new_task.tags not in [t.tags for t in self.task_G.nodes() if
                                     t.stage == new_task.stage], 'Duplicate set of tags detected in {0}'.format(
            new_task.stage)

        self.task_G.add_node(new_task)
        for p in parents:
            self.task_G.add_edge(p, new_task)

    def resolve_stage(self, stage):
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
                    raise ValueError(
                        "{0}.{1}'s tag's keys are not alphanumeric: {3}".format(stage, task, task.tags))

        return self

    def as_image(self, resolution='stage', save_to=None):
        """
        Writes the :class:`TaskGraph` as an image.
        gat
        :param path: the path to write to
        """
        import pygraphviz as pgv

        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname'] = "Courier"
        dag.node_attr['fontsize'] = 8
        dag.graph_attr['fontsize'] = 8
        dag.edge_attr['fontcolor'] = '#586e75'
        #dag.node_attr['fontcolor']='#586e75'
        dag.graph_attr['bgcolor'] = '#fdf6e3'

        if resolution == 'stage':
            dag.add_nodes_from([n.label for n in self.stage_G.nodes()])
            for u, v, attr in self.stage_G.edges(data=True):
                if isinstance(v.rel, many2one):
                    dag.add_edge(u.label, v.label, label=v.rel, style='dotted', arrowhead='odiamond')
                elif isinstance(v.rel, one2many):
                    dag.add_edge(u.label, v.label, label=v.rel, style='dashed', arrowhead='crow')
                else:
                    dag.add_edge(u.label, v.label, label=v.rel, arrowhead='vee')
        elif resolution == 'task':
            dag.add_nodes_from(self.task_G.nodes())
            dag.add_edges_from(self.task_G.edges())
            for stage, tasks in groupby(self.task_G.nodes(), lambda x: x.stage):
                sg = dag.add_subgraph(name="cluster_{0}".format(stage), label=stage.label, color='lightgrey')
        else:
            raise TypeError, '`resolution` must be `stage` or `task'

        dag.layout(prog="dot")
        return dag.draw(path=save_to, format='svg')


    def add_run(self, workflow, finish=True):
        """
        Shortcut to add to workflow and then run the workflow
        :param workflow: the workflow this dag will be added to
        :param finish: pass to workflow.run()
        """
        self.add_to_workflow(workflow)
        workflow.run_ready_tasks(finish=finish)

class DAGError(Exception): pass
class StageNameCollision(Exception): pass
class FlowFxnValidationError(Exception): pass

from .helpers import validate_name, validate_is_type_or_list

class Stage():
    def __init__(self, name, task=None, parents=None, rel=None, extra_tags=None, tasks=None, is_source=False):
        if parents is None:
            parents = []
        if tasks is None:
            tasks = []
        if tasks and task and not is_source:
            raise TypeError, 'cannot initialize with both a `task` and `tasks` unless `is_source`=True'
        if extra_tags is None:
            extra_tags = {}
        if rel == one2one or rel is None:
            rel = one2one()

        assert issubclass(task, Task), '`task` must be a subclass of `Task`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.task = task
        self.tasks = tasks
        self.parents = validate_is_type_or_list(parents, Stage)
        self.rel = rel
        self.is_source = is_source
        self.resolved = False

        self.extra_tags = extra_tags
        self.name = name
        self.is_finished = False

        validate_name(self.name, 'name')


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tasks))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)

