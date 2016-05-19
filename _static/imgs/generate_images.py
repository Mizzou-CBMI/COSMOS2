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

workflow = cosmos.start('One2One', '/tmp', check_output_dir=False)
stageA_tasks = workflow.add(ToolA(params=dict(i=i))
                             for i in [1, 2])
stageB_tasks = workflow.add(ToolB(params=task.params, parents=[task])
                             for task in stageA_tasks)
draw_task_graph(workflow.task_graph(), 'one2one.png', format='png')

workflow = cosmos.start('One2Many', '/tmp', check_output_dir=False)
stageA_tasks = workflow.add(ToolA(params=dict(i=i))
                             for i in [1, 2])
stageB_tasks = workflow.add(ToolB(params=dict(j=j, **task.params), parents=[task])
                             for task in stageA_tasks
                             for j in ['a','b'])
draw_task_graph(workflow.task_graph(), 'one2many.png', format='png')

workflow = cosmos.start('Many2One', '/tmp', check_output_dir=False)
stageA_tasks = workflow.add(ToolA(params=dict(i=i, j=j))
                             for i in [1, 2]
                             for j in ['a','b'])
get_i = lambda task: task.params['i']
stageB_tasks = workflow.add(ToolB(params=dict(i=i), parents=list(tasks))
                             for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
draw_task_graph(workflow.task_graph(), 'many2one.png', format='png')

workflow = cosmos.start('many2many', '/tmp', check_output_dir=False)
stageA_tasks = workflow.add(ToolA(params=dict(i=i, j=j))
                             for i in [1, 2]
                             for j in ['a','b'])
def B_generator(stageA_tasks):
    # For the more complicated relationships, it's usually best to just define a generator
    get_i = lambda task: task.params['i']
    for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
        parents = list(tasks)
        for k in ['x', 'y']:
            yield ToolB(params=dict(i=i, k=k), parents=parents)

stageB_tasks = workflow.add(B_generator(stageA_tasks))
draw_task_graph(workflow.task_graph(), 'many2many.png', format='png')