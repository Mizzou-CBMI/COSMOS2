.. _recipes:


Workflows
=================================

Workflows consist of a :term:`DAG` of Tasks.  Tasks are bundled into Stages, but Stages have almost no functionality
and are mostly just for keeping track of similar Tasks.  Tasks execute as soon as their dependencies have completed.

To create your :term:`DAG`, use :meth:`Workflow.add_task`. Python generators
and comprehensions are a great way to do this in a very readable way.

.. code-block:: python

    from cosmos import Cosmos

    def word_count(in_txt, out_txt, use_lines=False):
        l = ' -l' if use_lines else ''
        return r"""
            wc{l} {in_txt} > {out_txt}
            """.format(**locals())

    cosmos = Cosmos('sqlite:///cosmos.sqlite')
    cosmos.initdb()
    workflow = cosmos.start('My_Workflow', 'out_dir')

    wc_tasks = [ workflow.add_task(word_count, params=dict(in_txt=f),
                                                           uid=str(i))
                 for i,f in enumerate(('a.txt','b.txt')) ]


Each call to :meth:`Workflow.add_task` does the following:

1) Gets the corresponding Stage based on stage_name (which defaults to the name of of the `task function`, in this case "word_count")
2) Checks to see if a Task with the same *uid* already completed successfully in that stage
3) If `2)` is True, then return the successful Task instance (it will also be skipped when the `DAG` is run)
4) if `2)` is False, then create and return a new Task instance

This allows you to easily change the code that produced a failed Task and resume where you left off.

Creating Your Job Dependency Graph (DAG)
---------------------------------------------------
A useful model for thinking about how your stages and tasks are related is to think in terms of SQL relationship types.

One2one (aka. map)
+++++++++++++++++++++++++++++++
This is the most common type of dependency.  For each task in StageA, you create a single dependent task in StageB.


.. code-block:: python

    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start('One2One')
    for i in [1, 2]:
        stageA_task = workflow.add_task(tool_a, params=dict(i=i), uid=i)
        stageB_tasks = workflow.add_task(tool_b, params=task.params, parents=[task], uid=i)

    draw_task_graph(workflow.task_graph(), 'one2one.png')

.. figure:: /_static/imgs/one2one.png
    :align: center


One2many (aka. scatter)
+++++++++++++++++++++++++
For each parent task in StageA, two or more new children are generated in StageB.

.. code-block:: python

    workflow = cosmos.start('One2Many')
    for i in [1, 2]:
        stageA_task = workflow.add_task(tool_a, params=dict(i=i)), uid=i)
        for j in ['a','b']:
            stageB_tasks = workflow.add_task(tool_b,
                                             params=dict(j=j, **task.params),
                                             parents=[stageA_task],
                                             uid='%s_%s' % (i, j))
    draw_task_graph(workflow.task_graph(), 'one2many.png')


.. figure:: /_static/imgs/one2many.png
    :align: center



Many2one (aka. reduce or gather)
+++++++++++++++++++++++++++++++++
Two or more parents in StageA produce one task in StageB.

.. code-block:: python

    import itertools as it
    workflow = cosmos.start('Many2One')
    stageA_tasks = [workflow.add_task(tool_a, params=dict(i=i, j=j), uid='%s_%s'%(i,j))
                                      for i in [1, 2]
                                      for j in ['a','b'])]
    get_i = lambda task: task.params['i']
    stageB_task = workflow.add_task(tool_b, params=dict(i=i), parents=list(tasks), uid=i)
                                    for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
    draw_task_graph(workflow.task_graph(), 'many2one.png')


.. figure:: /_static/imgs/many2one.png
    :align: center

Many2many
+++++++++
Two or more parents in StageA produce two or more parents in StageB.

.. code-block:: python

    workflow = cosmos.start('many2many')
    stageA_tasks = [workflow.add_task(tool_a, params=dict(i=i, j=j), uid='%s_%s' %(i,j))
                                      for i in [1, 2]
                                      for j in ['a','b'])]
    def B_generator(stageA_tasks):
        # For the more complicated relationships, it's can be useful to define a generator
        get_i = lambda task: task.params['i']
        for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
            parents = list(tasks)
            for k in ['x', 'y']:
                yield workflow.add_task(tool_b, params=dict(i=i, k=k), parents=parents, uid='%s_%s' % (i,k))

    stageB_tasks = listB_generator(stageA_tasks))
    draw_task_graph(workflow.task_graph(), 'many2many.png')


.. figure:: /_static/imgs/many2many.png
    :align: center


API
-----------

Workflow
++++++++++++++

.. autoclass:: cosmos.api.Workflow
    :members: add_task, run
