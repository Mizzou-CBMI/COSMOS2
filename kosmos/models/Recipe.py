from .Stage import Stage
from .Task import INPUT
from . import rel

class Recipe(object):
    def __init__(self):
        import networkx as nx
        self.stage_G = nx.DiGraph()

    def add_source(self, tasks, name=None):
        assert isinstance(tasks, list), 'tasks must be a list'
        assert len(tasks) > 0, '`tasks` cannot be empty'
        if name is None:
            name = tasks[0].name
        tags = [tuple(t.tags.items()) for t in tasks]
        assert len(tags) == len(
            set(tags)), 'Duplicate inputs tags detected for {0}.  Tags within a stage must be unique.'.format(INPUT)

        stage = Stage(task_class=type(tasks[0]), tasks=tasks, parents=[], rel=None, name=name, is_source=True)
        for task in stage.tasks:
            task.stage = stage

        self.stage_G.add_node(stage)

        return stage

    def add_stage(self, task_class, parents, rel=rel.One2one, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        if name is None:
            if hasattr(task_class, 'name'):
                name = task_class.name
            else:
                name = task_class.__name__
        stage = Stage(name, task_class, parents, rel, extra_tags)

        assert stage.name not in [n.name for n in self.stage_G.nodes()], 'Duplicate stage names detected: {0}'.format(
            stage.name)

        self.stage_G.add_node(stage)
        for parent in stage.parents:
            self.stage_G.add_edge(parent, stage)

        return stage

    def save_image(self, save_to=None):
        import pygraphviz as pgv

        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname'] = "Courier"
        dag.node_attr['fontsize'] = 8
        dag.edge_attr['fontcolor'] = '#586e75'
        dag.graph_attr['bgcolor'] = '#fdf6e3'
        dag.add_nodes_from([n.label for n in self.stage_G.nodes()])
        for u, v, attr in self.stage_G.edges(data=True):
            if isinstance(v.rel, rel.Many2one):
                dag.add_edge(u.label, v.label, label=v.rel, style='dotted', arrowhead='odiamond')
            elif isinstance(v.rel, rel.One2many):
                dag.add_edge(u.label, v.label, label=v.rel, style='dashed', arrowhead='crow')
            else:
                dag.add_edge(u.label, v.label, label=v.rel, arrowhead='vee')

        dag.layout(prog="dot")
        return dag.draw(path=save_to, format='svg')