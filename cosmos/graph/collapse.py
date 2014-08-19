from networkx.algorithms import depth_first_search

from ..util.sqla import get_or_create
from .. import Task, Stage
import itertools as it

def _replace(session, G, bubble, new_node):
    """
    replace bubble with new_node in graph G.  NodesA must be a simple path, ie ie a->b->c
    handles ORM and task_graph operations
    :param type_: 'task' or 'stage'
    """
    # assert nodesA is a simple path?
    assert len(bubble) > 0

    head = bubble[0]
    tail = bubble[-1]
    for edge in list(head.incoming_edges):
        G.add_edge(edge.parent, new_node)
        if edge.parent in new_node.parents:
            # edge might already exist if new_node was loaded from DB
            edge.parent.outgoing_edges.remove(edge)
        else:
            edge.child = new_node

    for edge in list(tail.outgoing_edges):
        G.add_edge(new_node, edge.child)
        if edge.child in new_node.children:
            # edge might already exist if new_node was loaded from DB
            edge.child.incoming_edges.remove(edge)
        else:
            edge.parent = new_node

    G.remove_nodes_from(bubble)

    #get bubble and its remaining edges out of session
    # for node in bubble:
    #     for edge in list(it.chain(node.incoming_edges, node.outgoing_edges)):
    #         if edge in session:
    #             session.expunge(edge)
    #     if node in session:
    #         session.expunge(node)
    # session.add(new_node)



def _create_merged_task(tasks, new_stage):
    # Don't create a new task if one already exists and is successful
    successful_tasks = {frozenset(t.tags.items()): t for t in new_stage.tasks}  # successful because failed jobs have been deleted.

    def get_or_create_task(successful_tasks, tags, params):
        existing_task = successful_tasks.get(frozenset(tags.items()), None)
        if existing_task:
            return existing_task, True
        else:
            return Task(stage=new_stage, tags=tasks[-1].tags.copy(), **params), False


    def get_merged_drm(tasks):
        drms = set(t.drm for t in tasks if t.drm != 'local')
        assert len(drms) <= 1, "can't merge tasks with these drms: %s" % drms
        if len(drms) == 1:
            return drms.pop()
        else:
            return 'local'

    params = dict(
        mem_req=max(t.mem_req for t in tasks),
        time_req=max(t.time_req for t in tasks),
        cpu_req=max(t.cpu_req for t in tasks),
        must_succeed=any(t.must_succeed for t in tasks),
        output_dir=tasks[-1].output_dir,
        drm=get_merged_drm(tasks))

    replacement_task, created = get_or_create_task(successful_tasks, tasks[-1].tags, params)
    if created:
        for ifa in list(tasks[0]._input_file_assocs):
            ifa.task = replacement_task

        # TODO something about input files that are forwarded all the way through

        replacement_task.command = '\n\n'.join('### %s ###\n\n%s' % (t.stage.name, t.command) for t in tasks)

        # set output files
        for otf in tasks[-1].output_files:
            otf.task_output_for = replacement_task

    return replacement_task


def collapse(session, task_g, stage_g, recipe_stage_bubble, name):
    """
    :param G: a task_graph
    """
    # assume one2one
    # for each task_bubble in stage_bubble
    # create merged_node
    # replace node bubbles with merged_node
    # replace stage_bubble with merged_stage
    execution = task_g.nodes()[0].execution

    def create_stage(execution, name):
        stage, created = get_or_create(session=session, execution=execution, model=Stage, name=name)
        return stage

    def traverse_task_bubble(head_task, stage_bubble):
        # assumes a simple chain of nodes
        for task in depth_first_search.dfs_preorder_nodes(task_g, head_task):
            if task.stage in stage_bubble:
                yield task
            else:
                break

    stage_bubble = [rs.stage for rs in recipe_stage_bubble]

    new_stage = create_stage(execution, name)
    for head_task in stage_bubble[0].tasks:
        task_bubble = list(traverse_task_bubble(head_task, stage_bubble))
        merged_task = _create_merged_task(task_bubble, new_stage)
        _replace(session, task_g, task_bubble, merged_task)

    _replace(session, stage_g, stage_bubble, new_stage)

    #remove loose ends
    for stage in stage_bubble:
        stage.execution = None
        assert stage not in session
        for task in stage.tasks:
            # for tf in list(task.input_files):
            #     tf.tasks_input_for.remove(task)
            for ifa in list(task._input_file_assocs):
                ifa.taskfile = None
                assert ifa not in session
            #     if ifa in session:
            #         raise
            #         session.expunge(ifa)