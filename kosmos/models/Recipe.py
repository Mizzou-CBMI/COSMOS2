from .Task import INPUT, Task
from . import rel as _rel

import networkx as nx


class Recipe(object):
    def __init__(self):
        self.recipe_stage_G = nx.DiGraph()
        self.consumed = False

    def add_source(self, tasks, name=None):
        assert isinstance(tasks, list), 'tasks must be a list'
        assert len(tasks) > 0, '`tasks` cannot be empty'

        if name is None:
            name = tasks[0].__class__.__name__
        tags = [tuple(t.tags.items()) for t in tasks]
        assert len(tags) == len(
            set(tags)), 'Duplicate inputs tags detected for {0}.  Tags within a recipe_stage must be unique.'.format(
            INPUT)

        recipe_stage = RecipeStage(task_class=type(tasks[0]), tasks=tasks, rel=None, name=name, is_source=True)
        self.recipe_stage_G.add_node(recipe_stage)
        return recipe_stage

    def add_stage(self, task_class, parents, rel=_rel.One2one, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        assert isinstance(parents, list) or isinstance(parents, RecipeStage), \
            'parents must be a list of RecipeStages or a RecipeStage'
        if isinstance(parents, RecipeStage):
            parents = [parents]

        recipe_stage = RecipeStage(name, task_class, rel, extra_tags)

        assert recipe_stage.name not in [n.name for n in self.recipe_stage_G.nodes()], \
            'Duplicate recipe_stage names detected: %s' % recipe_stage.name

        self.recipe_stage_G.add_node(recipe_stage)
        for parent in parents:
            self.recipe_stage_G.add_edge(parent, recipe_stage)

        return recipe_stage

    def as_image(self, save_to=None):
        return recipe_image(self.recipe_stage_G, save_to)


def recipe_image(stage_graph, save_to=None):
    """
    :param stage_graph: recipe_stage_G or stage_G
    """

    import pygraphviz as pgv

    dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
    dag.node_attr['fontname'] = "Courier"
    dag.node_attr['fontsize'] = 8
    dag.edge_attr['fontcolor'] = '#586e75'

    for u, v, attr in stage_graph.edges(data=True):
        if isinstance(v.rel, _rel.Many2one):
            dag.add_edge(u.label, v.label, label=v.rel, style='dotted', arrowhead='odiamond')
        elif isinstance(v.rel, _rel.One2many):
            dag.add_edge(u.label, v.label, label=v.rel, style='dashed', arrowhead='crow')
        else:
            dag.add_edge(u.label, v.label, label=v.rel, arrowhead='vee')

    dag.layout(prog="dot")
    return dag.draw(path=save_to, format='svg')


class RecipeStage():
    ntasks = None

    def __init__(self, name, task_class=None, rel=None, extra_tags=None, tasks=None,
                 is_source=False):
        if name is None:
            if hasattr(task_class, 'name'):
                name = task_class.name
            else:
                name = task_class.__name__

        if tasks is None:
            tasks = []
        if tasks and task_class and not is_source:
            raise TypeError('cannot initialize with both a `task` and `tasks` unless `is_source`=True')
        if extra_tags is None:
            extra_tags = {}
        if rel == _rel.One2one or rel is None:
            rel = _rel.One2one()

        assert issubclass(task_class, Task), '`task` must be a subclass of `Task`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.properties = dict(name=name,
                               tasks=tasks,
                               task_class=task_class,
                               rel=rel,
                               is_source=is_source,
                               resolved=False,
                               extra_tags=extra_tags)
        self.__dict__.update(self.properties)

    @property
    def label(self):
        return '{0}'.format(self.name)

    def __repr__(self):
        return 'RecipeStage %s' % self.name or ''
