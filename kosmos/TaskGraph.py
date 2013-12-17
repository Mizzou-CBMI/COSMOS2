import itertools as it
import re
import networkx as nx

from .helpers import groupby, validate_is_type_or_list
from .Task import Task, INPUT

class Relationship(object):
    """Abstract Class for the various rel strategies"""

    def __str__(self):
        m = re.search("^(\w).+2(\w).+$", type(self).__name__)
        return '{0}2{1}'.format(m.group(1), m.group(2))


class one2one(Relationship):
    @classmethod
    def gen_tasks(klass, stage):
        for parent_task in it.chain(*[s.tasks for s in stage.parents]):
            tags2 = parent_task.tags.copy()
            tags2.update(stage.extra_tags)
            new_task = stage.task(stage=stage, tags=tags2)
            yield new_task, [parent_task]


class many2one(Relationship):
    def __init__(self, keywords):
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords

    @classmethod
    def gen_tasks(klass, stage):
        for tags, parent_task_group in many2one.reduce(stage, stage.rel.keywords):
            tags.update(stage.extra_tags)
            new_task = stage.task(stage=stage, tags=tags)
            yield new_task, parent_task_group

    @classmethod
    def reduce(cls, stage, keywords):
        if type(keywords) != list:
            raise TypeError('keywords must be a list')
        if any(k == '' for k in keywords):
            raise TypeError('keyword cannot be an empty string')

        parent_tasks = list(it.chain(*[s.tasks for s in stage.parents]))
        parent_tasks_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]),
                                                   parent_tasks)
        parent_tasks_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tasks)

        if len(parent_tasks_with_all_keywords) == 0:
            raise RelationshipError, 'Parent stages must have at least one task with all many2one keywords of {0}'.format(
                keywords)

        for tags, parent_task_group in groupby(parent_tasks_with_all_keywords,
                                               lambda t: dict((k, t.tags[k]) for k in keywords if k in t.tags)):
            parent_task_group = list(parent_task_group) + parent_tasks_without_all_keywords
            yield tags, parent_task_group


class one2many(Relationship):
    def __init__(self, split_by):
        one2many.validate_split_by(split_by)
        self.split_by = split_by

    @classmethod
    def validate_split_by(cls, split_by):
        assert isinstance(split_by, list), '`split_by` must be a list'
        if len(split_by) > 0:
            assert isinstance(split_by[0], tuple), '`split_by` must be a list of tuples'
            assert isinstance(split_by[0][0], str), 'the first element of tuples in `split_by` must be a str'
            assert isinstance(split_by[0][1],
                              list), 'the second element of the tuples in the `split_by` list must also be a list'

    @classmethod
    def gen_tasks(klass, stage):
        for parent_task in it.chain(*[s.tasks for s in stage.parents]):
            for tags in one2many.split(stage.rel.split_by):
                tags.update(parent_task.tags)
                tags.update(stage.extra_tags)
                new_task = stage.task(stage=stage, tags=tags)
                yield new_task, [parent_task]

    @classmethod
    def split(cls, split_by):
        splits = [list(it.product([split[0]], split[1])) for split in split_by]
        for new_tags in it.product(*splits):
            new_tags = dict(new_tags)
            yield new_tags


class many2many(Relationship):
    def __init__(self, keywords, split_by):
        one2many.validate_split_by(split_by)
        self.split_by = split_by
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords

    @classmethod
    def gen_tasks(klass, stage):
        for tags, parent_task_group in many2many.reduce_split(stage):
            tags.update(stage.extra_tags)
            new_task = stage.task(stage=stage, tags=tags)
            yield new_task, parent_task_group

    @classmethod
    def reduce_split(klass, stage):
        for tags, parent_task_group in many2one.reduce(stage, stage.rel.keywords):
            print tags, stage.rel.split_by
            for new_tags in one2many.split(stage.rel.split_by):
                new_tags.update(tags)
                yield new_tags, parent_task_group




class TaskGraph(object):
    """
    A Representation of a workflow as a :class:`TaskGraph` of jobs.
    """

    def __init__(self):
        """
        """
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
class RelationshipError(Exception): pass

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

        from .Task import Task
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

