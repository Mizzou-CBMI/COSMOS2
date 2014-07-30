import networkx as nx

from .. import RelationshipType, StageStatus
from . import rel as _rel


def isgenerator(iterable):
    return hasattr(iterable, '__iter__') and not hasattr(iterable, '__len__')


class Recipe(object):
    """
    A description of how to construct a taskgraph.  A taskgraph is a :term:`DAG` of tasks which describe job dependences.
    """

    def __init__(self):
        self.recipe_stage_G = nx.DiGraph()
        self.execution = None

    def add_source(self, tools, name=None):
        """
        Create a stage that has no parents

        :param tools: a list of Tool instances.
        """
        from .. import Tool

        if isgenerator(tools):
            tools = list(tools)
        elif hasattr(tools, '__class__') and issubclass(tools.__class__, Tool):
            tools = [tools]

        assert isinstance(tools, list) and len(tools) > 0, '`tools` must be a list of Tools, a Tool instance, or a generator of Tools'

        if name is None:
            name = tools[0].__class__.__name__
        tags = [tuple(t.tags.items()) for t in tools]
        assert len(tags) == len(
            set(
                tags)), 'Duplicate inputs tags detected for {0}, {1}.  Tags within a recipe_stage must be unique.'.format(
            name, tags)

        recipe_stage = RecipeStage(tool_class=type(tools[0]), source_tools=tools, rel=None, name=name, is_source=True)
        self.recipe_stage_G.add_node(recipe_stage)
        return recipe_stage

    def add_stage(self, tool_class, parents, rel=_rel.One2one, name=None, tag=None):
        """
        Creates a Stage

        :param tool_class: (Tool) a class which inherets from Tool.
        :param parents: (list) the stages which this tool depends on.
        :param rel: (Relationship) an instance of a Relationship.
        :param name: (str) a name for the stage, must be unique within this Recipe.
            The default is the name of the Tool's class.
        :param tags: (dict) extra tags to add to tools generated in this stage
        :returns: (RecipeStage) a RecipeStage which can then be used as a parent of another stage
        """
        assert isinstance(parents, list) or isinstance(parents, RecipeStage), \
            'parents must be a list of RecipeStages or a RecipeStage'
        if isinstance(parents, RecipeStage):
            parents = [parents]
        parents = filter(lambda p: p is not None, parents)
        from .. import Tool

        assert issubclass(tool_class, Tool), '`tool_class` must be a subclass of Tool'

        recipe_stage = RecipeStage(name, tool_class, rel, tag)

        assert recipe_stage.name not in [n.name for n in self.recipe_stage_G.nodes()], \
            'Duplicate recipe_stage names detected: %s' % recipe_stage.name

        self.recipe_stage_G.add_node(recipe_stage)
        for parent in parents:
            self.recipe_stage_G.add_edge(parent, recipe_stage)

        return recipe_stage

    def as_image(self, save_to=None):
        """
        Generate an svg image of this Recipe

        :param save_to:
        :returns:
        """
        g = stagegraph_to_agraph(self.recipe_stage_G)
        g.layout(prog="dot")
        return g.draw(path=save_to, format='svg')


def stagegraph_to_agraph(stage_graph):
    """
    :param stage_graph: recipe_stage_G or stage_G
    """

    import pygraphviz as pgv

    agraph = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
    agraph.node_attr['fontname'] = "Courier"
    agraph.node_attr['fontsize'] = 8
    agraph.edge_attr['fontcolor'] = '#586e75'

    status2color = {StageStatus.no_attempt: 'black',
                    StageStatus.running: 'navy',
                    StageStatus.successful: 'darkgreen',
                    StageStatus.failed: 'darkred'}
    rel2abbrev = {RelationshipType.one2one: 'o2o',
                  RelationshipType.one2many: 'o2m',
                  RelationshipType.many2one: 'm2o',
                  RelationshipType.many2many: 'm2m'}

    for stage in stage_graph.nodes():
        agraph.add_node(stage, color=status2color.get(getattr(stage, 'status', None), 'black'),
                        URL=stage.url, label=stage.label)

    for u, v in stage_graph.edges():
        if v.relationship_type == RelationshipType.many2one:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), style='dotted', arrowhead='odiamond')
        elif v.relationship_type == RelationshipType.one2many:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), style='dashed', arrowhead='crow')
        else:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), arrowhead='vee')

    return agraph


def stages_to_image(stages, path=None):
    """
    Creates an SVG image of Stages or RecipeStages and their dependencies.
    """
    g = nx.DiGraph()
    g.add_nodes_from(stages)
    g.add_edges_from([(parent, stage) for stage in stages for parent in stage.parents])

    g = stagegraph_to_agraph(g)
    g.layout(prog="dot")
    return g.draw(path=path, format='svg')


class RecipeStage():
    """
    A stage that belongs to a Recipe.
    """
    ntasks = None

    def __init__(self, name, tool_class=None, rel=None, extra_tags=None, source_tools=None,
                 is_source=False):
        if name is None:
            if hasattr(tool_class, 'name'):
                name = tool_class.name
            else:
                name = tool_class.__name__

        if source_tools is None:
            source_tools = []
        if source_tools and tool_class and not is_source:
            raise TypeError('cannot initialize with both a `tool` and `tools` unless `is_source`=True')
        if extra_tags is None:
            extra_tags = {}
        if rel == _rel.One2one or rel is None:
            rel = _rel.One2one()
        elif rel == _rel.Many2one:
            rel = _rel.Many2one()

        from .Tool import Tool

        assert issubclass(tool_class, Tool), '`tool` must be a subclass of `Tool`'
        assert isinstance(rel, _rel.Relationship), '`rel` must be of type `Relationship`'
        assert isinstance(extra_tags, dict), '`extra_tags` must be of type `dict`'

        self.properties = dict(name=name,
                               source_tools=source_tools,
                               tool_class=tool_class,
                               rel=rel,
                               is_source=is_source,
                               resolved=False,
                               extra_tags=extra_tags,
                               relationship_type=rel.type)
        self.__dict__.update(self.properties)

    @property
    def label(self):
        ntools = ' (x%s)' % len(self.source_tools) if self.source_tools else ''
        return '{0}{1}'.format(self.name, ntools)

    @property
    def url(self):
        return ''

    def __repr__(self):
        return '<RecipeStage %s>' % self.name or ''
