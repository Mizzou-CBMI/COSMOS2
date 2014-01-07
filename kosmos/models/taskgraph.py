import re
import copy

from ..util.helpers import groupby
from ..util.sqla import get_or_create
from .. import TaskStatus
from .Stage import Stage
import functools

from sqlalchemy import inspect
import networkx as nx


def render_recipe(execution, recipe):
    task_g = nx.DiGraph()
    session = inspect(execution).session
    existing_tasks = {(t.stage, frozenset(t.tags.items())): t for t in execution.tasks}

    # This replicates the recipe_stage_G into a stage_G of Stage objects rather than RecipeStages
    f = functools.partial(_recipe_stage2stage, execution=execution, existing_tasks=existing_tasks)
    stage_g = nx.relabel_nodes(recipe.recipe_stage_G, f, copy=True)

    for stage in nx.topological_sort(stage_g):
        stage.parents = stage_g.predecessors(stage)
        if not stage.resolved:
            if stage.is_source:
                for manually_instantiated_class in stage.recipe_stage.tasks:
                    existing_task = existing_tasks.get((stage, frozenset(manually_instantiated_class.tags.items())), None)
                    if existing_task:
                        task_g.add_node(existing_task)
                    else:
                        stage.tasks.append(manually_instantiated_class)
                        task_g.add_node(manually_instantiated_class)

            else:
                for new_task, parent_tasks in stage.rel.__class__.gen_tasks(stage):
                    existing_task = existing_tasks.get((stage, frozenset(new_task.tags.items())), None)
                    if existing_task:
                        new_task = existing_task
                    else:
                        stage.tasks.append(new_task)

                    task_g.add_edges_from([(p, new_task) for p in parent_tasks])
        stage.resolved = True
        #TODO: assert no duplicate tags
    return task_g, stage_g


def dag_from_tasks(tasks):
    g = nx.DiGraph()
    g.add_nodes_from(tasks)
    g.add_edges_from([(parent, task) for task in tasks for parent in task.parents])
    return g


def createAGraph(taskgraph):
    import pygraphviz as pgv

    agraph = pgv.AGraph(strict=False, directed=True, fontname="Courier")
    agraph.node_attr['fontname'] = "Courier"
    agraph.node_attr['fontcolor'] = '#000'
    agraph.node_attr['fontsize'] = 8
    agraph.graph_attr['fontsize'] = 8
    agraph.edge_attr['fontcolor'] = '#586e75'
    #dag.graph_attr['bgcolor'] = '#fdf6e3'

    agraph.add_edges_from(taskgraph.task_g.edges())
    for stage, tasks in groupby(taskgraph.task_g.nodes(), lambda x: x.stage):
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


def as_image(taskgraph, path=None):
    g = createAGraph(taskgraph)
    g.layout(prog="dot")
    return g.draw(path=path, format='svg')


def _recipe_stage2stage(recipe_stage, execution, existing_tasks):
    """
    Creates a Stage object from a RecipeStage object
    """
    session = inspect(execution).session
    stage, created = get_or_create(session=session, model=Stage, name=recipe_stage.name,
                                   execution=execution)
    session.commit()

    if not created:
        execution.log.info('loaded %s' % stage)
    else:
        execution.log.info('created %s' % stage)

    for k, v in recipe_stage.properties.items():
        if k != 'tasks':
            setattr(stage, k, v)
    stage.recipe_stage = recipe_stage
    session.add(stage)
    session.commit()
    return stage