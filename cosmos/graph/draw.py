#
# Task stuff
#

from ..util.helpers import groupby2
from .. import TaskStatus

try:
    import pygraphviz as _

    pygraphviz_available = True
except ImportError:
    pygraphviz_available = False


def draw_task_graph(task_graph, save_to=None, format='svg', url=False):
    a = taskgraph_to_agraph(task_graph, url=url)
    a.layout('dot')
    return a.draw(path=save_to, format=format)


def taskgraph_to_agraph(task_graph, url=True):
    """
    Converts a networkx graph into a pygraphviz Agraph
    """
    import pygraphviz as pgv

    agraph = pgv.AGraph(strict=False, directed=True, fontname="Courier")
    agraph.node_attr['fontname'] = "Courier"
    # agraph.node_attr['fontcolor'] = '#000000'
    agraph.node_attr['fontsize'] = 8
    agraph.graph_attr['fontsize'] = 8
    agraph.edge_attr['fontcolor'] = '#586e75'

    agraph.add_edges_from(task_graph.edges())
    for stage, tasks in groupby2(task_graph.nodes(), lambda x: x.stage):
        sg = agraph.add_subgraph(name="cluster_{0}".format(stage), label=str(stage), color='grey', style='dotted')
        for task in tasks:
            def truncate_val(kv):
                v = "{0}".format(kv[1])
                v = v if len(v) < 10 else v[1:8] + '..'
                return "{0}: {1}".format(kv[0], v)

            label = " \\n".join(map(truncate_val, task.tags.items()))
            status2color = {TaskStatus.no_attempt: 'black',
                            TaskStatus.waiting: 'gold1',
                            TaskStatus.submitted: 'navy',
                            TaskStatus.successful: 'darkgreen',
                            TaskStatus.failed: 'darkred',
                            TaskStatus.killed: 'darkred'}

            sg.add_node(task, label=label, URL=task.url if url else '#', target="_blank",
                        color=status2color.get(task.status, 'black'))

    return agraph


def taskgraph_to_image(taskgraph, path=None, url=False):
    taskgraph.layout(prog="dot")
    agraph = taskgraph_to_agraph(taskgraph, url=url)
    return agraph.draw(path=path, format='svg')


#
# Stage stuff
#
from .. import RelationshipType
from ..models.Stage import StageStatus


def draw_stage_graph(stage_graph, save_to=None, url=False, format='svg'):
    g = stagegraph_to_agraph(stage_graph, url=url)
    g.layout(prog="dot")
    return g.draw(path=save_to, format=format)


def stagegraph_to_agraph(stage_graph, url=True):
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
                        URL=stage.url if url else '', label=stage.label)

    for u, v in stage_graph.edges():
        v.relationship_type = None
        if v.relationship_type == RelationshipType.many2one:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), style='dotted', arrowhead='odiamond')
        elif v.relationship_type == RelationshipType.one2many:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), style='dashed', arrowhead='crow')
        else:
            agraph.add_edge(u, v, label=rel2abbrev.get(v.relationship_type, ''), arrowhead='vee')

    return agraph
