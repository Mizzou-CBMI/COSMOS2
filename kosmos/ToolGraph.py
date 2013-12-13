import itertools as it
import re

import networkx as nx

from .helpers import groupby, validate_is_type_or_list
#from .Task import Task, TaskError
from .Tool import Tool, INPUT
from .helpers import validate_name

class Relationship(object):
    """Abstract Class for the various rel strategies"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        m = re.search("^(\w).+2(\w).+$", type(self).__name__)
        return '{0}2{1}'.format(m.group(1), m.group(2))


class one2one(Relationship):
    pass


class many2one(Relationship):
    def __init__(self, keywords, *args, **kwargs):
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords
        super(Relationship, self).__init__(*args, **kwargs)


class one2many(Relationship):
    def __init__(self, split_by, *args, **kwargs):
        assert isinstance(split_by, list), '`split_by` must be a list'
        if len(split_by) > 0:
            assert isinstance(split_by[0], tuple), '`split_by` must be a list of tuples'
            assert isinstance(split_by[0][0], str), 'the first element of tuples in `split_by` must be a str'
            assert isinstance(split_by[0][1],
                              list), 'the second element of the tuples in the `split_by` list must also be a list'

        self.split_by = split_by
        super(Relationship, self).__init__(*args, **kwargs)


class ToolGraph(object):
    """
    A Representation of a workflow as a :class:`ToolGraph` of jobs.
    """

    def __init__(self, cpu_req_override=False, mem_req_factor=1):
        """
        :param cpu_req_override: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        :param dag.ignore_stage_name_collisions:  Allows the flowfxns to add to stages that already exists.
        """
        self.tool_G = nx.DiGraph()
        self.stage_G = nx.DiGraph()
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor

    def source(self, tools, name=None):
        assert isinstance(tools, list), 'tools must be a list'
        assert len(tools) > 0, '`tools` cannot be empty'
        if name is None:
            name = tools[0].name
        tags = [tuple(t.tags.items()) for t in tools]
        assert len(tags) == len(
            set(tags)), 'Duplicate inputs tags detected for {0}.  Tags within a stage must be unique.'.format(INPUT)

        stage = ToolStage(tool=type(tools[0]), tools=tools, parents=[], rel=None, name=name, is_source=True)
        for tool in stage.tools:
            tool.stage = stage

        self.stage_G.add_node(stage)

        return stage


    def stage(self, tool, parents, rel=one2one, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        if name is None:
            if hasattr(tool, 'name'):
                name = tool.name
            else:
                name = tool.__name__
        stage = ToolStage(name, tool, parents, rel, extra_tags)

        assert stage.name not in [n.name for n in self.stage_G.nodes()], 'Duplicate stage names detected: {0}'.format(
            stage.name)

        self.stage_G.add_node(stage)
        for parent in stage.parents:
            self.stage_G.add_edge(parent, stage)

        return stage

    def resolve(self, settings={}, parameters={}):
        self._resolve_tools()
        self.configure(settings, parameters)
        return self

    def _add_tool_to_tool_G(self, new_tool, parents=None):
        if parents is None:
            parents = []
        assert new_tool.tags not in [t.tags for t in self.tool_G.nodes() if
                                     t.stage == new_tool.stage], 'Duplicate set of tags detected in {0}'.format(
            new_tool.stage)

        self.tool_G.add_node(new_tool)
        for p in parents:
            self.tool_G.add_edge(p, new_tool)

    def _resolve_tools(self):
        for stage in nx.topological_sort(self.stage_G):
            if stage.is_source:
                #stage.tools is already set
                for tool in stage.tools:
                    self._add_tool_to_tool_G(tool)

            elif isinstance(stage.rel, one2one):
                for parent_tool in it.chain(*[s.tools for s in stage.parents]):
                    tags2 = parent_tool.tags.copy()
                    tags2.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage, dag=self, tags=tags2)
                    stage.tools.append(new_tool)
                    self._add_tool_to_tool_G(new_tool, [parent_tool])

            elif isinstance(stage.rel, many2one):
                keywords = stage.rel.keywords
                if type(keywords) != list:
                    raise TypeError('keywords must be a list')
                if any(k == '' for k in keywords):
                    raise TypeError('keyword cannot be an empty string')

                parent_tools = list(it.chain(*[s.tools for s in stage.parents]))
                parent_tools_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]),
                                                           parent_tools)
                parent_tools_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tools)

                if len(
                        parent_tools_with_all_keywords) == 0: raise RelationshipError, 'Parent stages must have at least one tool with all many2one keywords of {0}'.format(
                    keywords)

                for tags, parent_tool_group in groupby(parent_tools_with_all_keywords,
                                                       lambda t: dict((k, t.tags[k]) for k in keywords if k in t.tags)):
                    parent_tool_group = list(parent_tool_group) + parent_tools_without_all_keywords
                    tags.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage, dag=self, tags=tags)
                    stage.tools.append(new_tool)
                    self._add_tool_to_tool_G(new_tool, parent_tool_group)

            elif isinstance(stage.rel, one2many):
                parent_tools = list(it.chain(*[s.tools for s in stage.parents]))
                #: splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
                splits = [list(it.product([split[0]], split[1])) for split in stage.rel.split_by]
                for parent_tool in parent_tools:
                    for new_tags in it.product(*splits):
                        tags = dict(parent_tool.tags).copy()
                        tags.update(stage.extra_tags)
                        tags.update(dict(new_tags))
                        new_tool = stage.tool(stage=stage, dag=self, tags=tags)
                        stage.tools.append(new_tool)
                        self._add_tool_to_tool_G(new_tool, [parent_tool])

            else:
                raise AssertionError, 'Stage constructed improperly'

        for tool in self.tool_G:
            for key in tool.tags:
                if not re.match('\w', key):
                    raise ValueError("{0}.{1}'s tag's keys are not alphanumeric: {3}".format(stage, tool, tool.tags))

        return self

    def as_image(self, resolution='stage', save_to=None):
        """
        Writes the :class:`ToolGraph` as an image.
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
        elif resolution == 'tool':
            dag.add_nodes_from(self.tool_G.nodes())
            dag.add_edges_from(self.tool_G.edges())
            for stage, tools in groupby(self.tool_G.nodes(), lambda x: x.stage):
                sg = dag.add_subgraph(name="cluster_{0}".format(stage), label=stage.label, color='lightgrey')
        else:
            raise TypeError, '`resolution` must be `stage` or `tool'

        dag.layout(prog="dot")
        return dag.draw(path=save_to, format='svg')

    def configure(self, settings={}, parameters={}):
        """
        Sets the parameters an settings of every tool in the dag.

        :param parameters: (dict) {'stage_name': { 'name':'value', ... }, {'stage_name2': { 'key':'value', ... } }
        :param settings: (dict) { 'key':'val'} }
        """
        self.parameters = parameters
        for tool in self.tool_G.node:
            tool.settings = settings
            if tool.stage.name not in self.parameters:
                #set defaults, then override with parameters
                self.parameters[tool.stage.name] = tool.default_params.copy()
                self.parameters[tool.stage.name].update(parameters.get(tool.__class__.__name__, {}))
                self.parameters[tool.stage.name].update(parameters.get(tool.stage.name, {}))
            tool.parameters = self.parameters.get(tool.stage.name, {})
        return self

    def add_run(self, workflow, finish=True):
        """
        Shortcut to add to workflow and then run the workflow
        :param workflow: the workflow this dag will be added to
        :param finish: pass to workflow.run()
        """
        self.add_to_workflow(workflow)
        workflow.run(finish=finish)

class ToolStage():
    def __init__(self, name, tool=None, parents=None, rel=None, extra_tags=None, tools=None, is_source=False):
        if parents is None:
            parents = []
        if tools is None:
            tools = []
        if tools and tool and not is_source:
            raise TypeError, 'cannot initialize with both a `tool` and `tools` unless `is_source`=True'
        if extra_tags is None:
            extra_tags = {}
        if rel == one2one or rel is None:
            rel = one2one()

        assert issubclass(tool, Tool), '`tool` must be a subclass of `Tool`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.tool = tool
        self.tools = tools
        self.parents = validate_is_type_or_list(parents, ToolStage)
        self.rel = rel
        self.is_source = is_source

        self.extra_tags = extra_tags
        self.name = name

        validate_name(self.name, 'name')


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tools))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)


class DAGError(Exception): pass
class StageNameCollision(Exception): pass
class FlowFxnValidationError(Exception): pass
class RelationshipError(Exception): pass
