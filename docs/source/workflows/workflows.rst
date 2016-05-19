.. _recipes:


Workflows
=================================

Workflows consist of a :term:`DAG` of Tasks.  Tasks are bundled into Stages, but Stages have almost no functionality
and are mostly just for keeping track of similar Tasks.  Tasks execute as soon as their dependencies have completed.

To create your :term:`DAG`, use :meth:`Workflow.add_task`. Python generators
and comprehensions are a great way to do this in a very readable way.

.. code-block:: python

    from cosmos import Cosmos

    def word_count(use_lines=False, in_txt=find('txt$'), out_txt=out_dir('count.txt')):
        l = ' -l' if use_lines else ''
        return r"""
            wc{l} {in_txt} > {out_txt}
            """.format(**locals())

    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start('My_Workflow', 'out_dir)

    # note in_txt is specified, so find() will not be used.
    wc_tasks = [ workflow.add_task(word_count, params=dict(in_txt='a.txt')) ]


Each call to :meth:`Workflow.add_task` does the following:

1) Gets the corresponding Stage based on stage_name (which defaults to the name of of the `cmd_fxn`)
2) Checks to see if a Task with the same params already completed successfully in that stage
3) If `2)` is True, then return that Task instance (it will also be skipped when the `DAG` is run)
4) if `2)` is False, then create and return new Task instance


Tags
-----

Every instance of a Task has a ``dict`` of params.  These params are used for the following:

* A unique identifier.  No tool/task can have the same set of params within the *same stage*.
* Parameters.  If a keyword in a tool's params matches a parameter in it's ``cmd()`` method, it will be passed into the call to ``cmd()`` as a parameter.
  For example when ``cmd()`` is called by Cossmos for the tool ``WordCount(params=dict(lines=True, other='val'))``, it will be called like this:
  ``cmd(lines=True, other='val',...)``.
* A way to group similar tasks together when defining the :term:`DAG`.
* A way to look up particular tasks in the Web Interface or using the API.


Creating Your Job Dependency Graph (DAG)
---------------------------------------------------
A useful model for thinking about how your stages are related is to think in terms of SQL relationship types.

One2one (aka a map() operation)
+++++++++++++++++++++++++++++++
This is the most common stage dependency.  For each task in StageA, you create a single dependent task in StageB.


.. code-block:: python

    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start('One2One', '/tmp', check_output_dir=False)
    stageA_tasks = workflow.add_task(tool_a, params=dict(i=i))
                                 for i in [1, 2])
    stageB_tasks = workflow.add_task(tool_b, ags=task.params, parents=[task])
                                 for task in stageA_tasks)
    draw_task_graph(workflow.task_graph(), 'one2one.png')

.. figure:: /_static/imgs/one2one.png
    :align: center


One2many
++++++++
For each parent task in StageA, two or more new children are generated in StageB

.. code-block:: python

    workflow = cosmos.start('One2Many', '/tmp', check_output_dir=False)
    stageA_tasks = workflow.add_task(tool_a, params=dict(i=i))
                                      for i in [1, 2])
    stageB_tasks = workflow.add_task(tool_b, params=dict(j=j, **task.params), parents=[task])
                                      for task in stageA_tasks
                                      for j in ['a','b'])
    draw_task_graph(workflow.task_graph(), 'one2many.png')


.. figure:: /_static/imgs/one2many.png
    :align: center



Many2one
++++++++++++++++++++++++++
Two or more parents in StageA produce one task in StageB.

.. code-block:: python

    workflow = cosmos.start('Many2One', '/tmp', check_output_dir=False)
    stageA_tasks = workflow.add_task(tool_a, params=dict(i=i, j=j))
                                      for i in [1, 2]
                                      for j in ['a','b'])
    get_i = lambda task: task.params['i']
    stageB_tasks = workflow.add_task(tool_b, params=dict(i=i), parents=list(tasks))
                                      for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
    draw_task_graph(workflow.task_graph(), 'many2one.png')


.. figure:: /_static/imgs/many2one.png
    :align: center

Many2many
+++++++++
Two or more parents in StageA produce two or more parents in StageB.

.. code-block:: python

    workflow = cosmos.start('many2many', '/tmp', check_output_dir=False)
    stageA_tasks = workflow.add_task(tool_a, params=dict(i=i, j=j))
                                      for i in [1, 2]
                                      for j in ['a','b'])
    def B_generator(stageA_tasks):
        # For the more complicated relationships, it's usually best to just define a generator
        get_i = lambda task: task.params['i']
        for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
            parents = list(tasks)
            for k in ['x', 'y']:
                yield tool_b, params=dict(i=i, k=k), parents=parents)

    stageB_tasks = workflow.add_task(B_generator(stageA_tasks))
    draw_task_graph(workflow.task_graph(), 'many2many.png')


.. figure:: /_static/imgs/many2many.png
    :align: center


API
-----------

Workflow
++++++++++++++

.. autoclass:: cosmos.api.Workflow
    :members: add_task, run
