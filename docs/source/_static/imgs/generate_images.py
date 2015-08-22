from cosmos import Cosmos, Tool, NOOP, draw_task_graph
import itertools as it


class ToolA(Tool):
    def cmd(self):
        return NOOP


class ToolB(Tool):
    def cmd(self):
        return NOOP


class ToolC(Tool):
    def cmd(self):
        return NOOP


cosmos = Cosmos()
cosmos.initdb()

execution = cosmos.start('One2One', '/tmp', check_output_dir=False)
stageA_tasks = execution.add(ToolA(tags=dict(i=i))
                             for i in [1, 2])
stageB_tasks = execution.add(ToolB(tags=task.tags, parents=[task])
                             for task in stageA_tasks)
draw_task_graph(execution.task_graph(), 'one2one.png', format='png')

execution = cosmos.start('One2Many', '/tmp', check_output_dir=False)
stageA_tasks = execution.add(ToolA(tags=dict(i=i))
                             for i in [1, 2])
stageB_tasks = execution.add(ToolB(tags=dict(j=j, **task.tags), parents=[task])
                             for task in stageA_tasks
                             for j in ['a','b'])
draw_task_graph(execution.task_graph(), 'one2many.png', format='png')

execution = cosmos.start('Many2One', '/tmp', check_output_dir=False)
stageA_tasks = execution.add(ToolA(tags=dict(i=i, j=j))
                             for i in [1, 2]
                             for j in ['a','b'])
get_i = lambda task: task.tags['i']
stageB_tasks = execution.add(ToolB(tags=dict(i=i), parents=list(tasks))
                             for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
draw_task_graph(execution.task_graph(), 'many2one.png', format='png')

execution = cosmos.start('many2many', '/tmp', check_output_dir=False)
stageA_tasks = execution.add(ToolA(tags=dict(i=i, j=j))
                             for i in [1, 2]
                             for j in ['a','b'])
def B_generator(stageA_tasks):
    # For the more complicated relationships, it's usually best to just define a generator
    get_i = lambda task: task.tags['i']
    for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
        parents = list(tasks)
        for k in ['x', 'y']:
            yield ToolB(tags=dict(i=i, k=k), parents=parents)

stageB_tasks = execution.add(B_generator(stageA_tasks))
draw_task_graph(execution.task_graph(), 'many2many.png', format='png')