from collections import namedtuple
import networkx as nx

from .draw import draw_stage_graph
from . import rel as _rel


def isgenerator(iterable):
    return hasattr(iterable, '__iter__') and not hasattr(iterable, '__len__')


# Collapsed_Stage = namedtuple('Collapsed_Stage', ['stages', 'name'])


class Recipe(object):
    """
    A description of how to construct a taskgraph.  A taskgraph is a :term:`DAG` of tasks which describe job dependences.
    """

    def __init__(self):
        self.recipe_stage_G = nx.DiGraph()
        self.execution = None
        # self.collapses = []

    def add(self, tools, name=None):
        from .. import Tool

        if isgenerator(tools):
            tools = list(tools)
        elif hasattr(tools, '__class__') and issubclass(tools.__class__, Tool):
            tools = [tools]

        assert isinstance(tools, list) and all(issubclass(t.__class__, Tool) for t in
                                               tools), '`tools` must be a list of Tools, a Tool instance, or a generator of Tools'
        assert len(tools) > 0, '`tools` cannot be an empty list'

        if name is None:
            name = tools[0].__class__.__name__
        tags = [tuple(t.tags.items()) for t in tools]
        assert len(tags) == len(set(tags)), 'Duplicate tags detected for {0}, {1}.  ' \
                                            'Tags within a recipe_stage must be unique.'.format(name, map(dict, tags))

        recipe_stage = RecipeStage(tool_class=type(tools[0]), source_tools=tools, rel=None, name=name)
        self.recipe_stage_G.add_node(recipe_stage)
        self._validate_not_duplicate_name(recipe_stage.name)
        return recipe_stage


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

        assert isinstance(tools, list) and all(issubclass(t.__class__, Tool) for t in
                                               tools), '`tools` must be a list of Tools, a Tool instance, or a generator of Tools'
        assert len(tools) > 0, '`tools` cannot be an empty list'

        if name is None:
            name = tools[0].__class__.__name__
        tags = [tuple(t.tags.items()) for t in tools]
        assert len(tags) == len(
            set(
                tags)), 'Duplicate inputs tags detected for {0}, {1}.  Tags within a recipe_stage must be unique.'.format(
            name, map(dict, tags))

        recipe_stage = RecipeStage(tool_class=type(tools[0]), source_tools=tools, rel=None, name=name, is_source=True)
        self._validate_not_duplicate_name(recipe_stage.name)
        self.recipe_stage_G.add_node(recipe_stage)
        return recipe_stage

    def add_stage(self, tool_class, parents, rel=_rel.One2one, tag=None, out='', name=None):
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
        if isinstance(parents, RecipeStage):
            parents = [parents]
        assert isinstance(parents, list) and all(isinstance(p, RecipeStage) for p in parents), \
            'parents must be a list of RecipeStages or a RecipeStage'
        parents = filter(lambda p: p is not None, parents)
        from .. import Tool

        assert issubclass(tool_class, Tool), '`tool_class` must be a subclass of Tool'
        assert len(parents), 'must have at least one parent for %s' % name

        recipe_stage = RecipeStage(name, tool_class, rel, tag, output_dir_pre_interpolation=out)

        self._validate_not_duplicate_name(recipe_stage.name)

        self.recipe_stage_G.add_node(recipe_stage)
        for parent in parents:
            self.recipe_stage_G.add_edge(parent, recipe_stage)

        return recipe_stage

    # def collapse_stages(self, stages, name=None):
    #     # assert stages are collapsible
    #     assert isinstance(stages, list), '`stages` must be a list'
    #     self._validate_not_duplicate_name(name)
    #     stages = filter(bool, stages)
    #     if len(stages) > 1:
    #         if name is None:
    #             '__'.join(s.name for s in stages)
    #
    #         self.collapses.append(Collapsed_Stage(stages, name))

    def _validate_not_duplicate_name(self, name):
        assert name not in [n.name for n in
                            self.recipe_stage_G.nodes()], 'Duplicate recipe_stage names detected: %s' % name

    def as_image(self, save_to=None):
        """
        Generate an svg image of this Recipe

        :param save_to:
        :returns:
        """
        return draw_stage_graph(self.recipe_stage_G, save_to=save_to)


class RecipeStage():
    """
    A stage that belongs to a Recipe.
    """
    ntasks = None

    def __init__(self, name, tool_class=None, rel=None, extra_tags=None, source_tools=None,
                 is_source=False, output_dir_pre_interpolation=''):
        if name is None:
            if hasattr(tool_class, 'name'):
                name = tool_class.name
            else:
                name = tool_class.__name__

        if source_tools is None:
            source_tools = []
        # if source_tools and tool_class and not is_source:
        #     raise TypeError('cannot initialize with both a `tool` and `tools` unless `is_source`=True')
        if extra_tags is None:
            extra_tags = {}
        if rel == _rel.One2one or rel is None:
            rel = _rel.One2one()
        elif rel == _rel.Many2one:
            rel = _rel.Many2one()

        from .. import Tool

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
                               relationship_type=rel.type,
                               output_dir_pre_interpolation=output_dir_pre_interpolation)
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
