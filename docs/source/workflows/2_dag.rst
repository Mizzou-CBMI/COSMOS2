.. _recipes:


DAG (Directed Acyclic Graph)
=================================

Executions costs of a :term:`DAG` of Tasks.  Tasks execute as soon as their dependencies have completed.

To create your :term:`DAG`, add instances of your :class:`Tool` and specify their dependencies.  Python generators
and comprehensions are a great way to do this in a very readable way.


.. code-block:: python

    from cosmos import Cosmos

    cosmos = Cosmos()
    cosmos.initdb()
    execution = cosmos.start('My_Workflow', 'out_dir)
    stageA = execution.add([Input('/path/to/file.txt', tags=dict(file='1')),
                            Input('/path/to/file2.txt', tags=dict(file='2'))],
                            name="Load_Input_Files")


Each call to execution.add() creates a new stage (which must have a unique name, the default is the name of the first Tool added).
Stages are only for the user's mental organization and visualization of a group of Tasks, and has nothing to do with how the DAG gets executed.

Input
-------

An :class:`Input` is just a type of Tool that does nothing but outputs the file you specified.  :class:`Inputs` can
be used if you'd like a single Input Task to output multiple files.

Creating Tasks and Dependencies (Nodes and Edges)
---------------------------------------------------
A useful model for thinking about how your stages are related is to think in terms of SQL relationship types.

One2one
+++++++
This is the most common stage dependency.  For each task in StageA, you create a single dependent task in StageB.


.. code-block:: python

    cosmos = Cosmos()
    cosmos.initdb()
    execution = cosmos.start('One2One', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add(ToolA(tags=dict(i=i))
                                 for i in [1, 2])
    stageB_tasks = execution.add(ToolB(tags=task.tags, parents=[task])
                                 for task in stageA_tasks)
    draw_task_graph(execution.task_graph(), 'one2one.png')

.. figure:: /_static/imgs/one2one.png
    :align: center


One2many
++++++++
For each parent task in StageA, two or more new children are generated in StageB

.. code-block:: python

    execution = cosmos.start('One2Many', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add(ToolA(tags=dict(i=i))
                                 for i in [1, 2])
    stageB_tasks = execution.add(ToolB(tags=dict(j=j, **task.tags), parents=[task])
                                 for task in stageA_tasks
                                 for j in [1, 2])
    draw_task_graph(execution.task_graph(), 'one2many.png')


.. figure:: /_static/imgs/one2many.png
    :align: center



Many2one
+++++++++
Two or more parents in StageA produce one task in StageB.

.. code-block:: python

    execution = cosmos.start('Many2One', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add(ToolA(tags=dict(i=i, j=j))
                                 for i in [1, 2]
                                 for j in [1, 2])
    get_i = lambda task: task.tags['i']
    stageB_tasks = execution.add(ToolB(tags=dict(i=i), parents=list(tasks))
                                 for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
    draw_task_graph(execution.task_graph(), 'many2one.png')


.. figure:: /_static/imgs/many2one.png
    :align: center

Many2many
+++++++++
Two or more parents in StageA produce two or more parents in StageB.

.. code-block:: python

    execution = cosmos.start('many2many', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add(ToolA(tags=dict(i=i, j=j))
                                 for i in [1, 2]
                                 for j in [1, 2])
    def B_generator(stageA_tasks):
        # For the more complicated relationships, it's usually best to just define a generator
        get_i = lambda task: task.tags['i']
        for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
            parents = list(tasks)
            for k in ['x', 'y']:
                yield ToolB(tags=dict(i=i, k=k), parents=parents)

    stageB_tasks = execution.add(B_generator(stageA_tasks))
    draw_task_graph(execution.task_graph(), 'many2many.png')


.. figure:: /_static/imgs/many2many.png
    :align: center

Helpers
++++++++++

The above patterns are very common, so there are some handy generators available that take care of many cases.
However, it is not
recommended you use these until you are familiar with creating the DAG more explicitly.  Feel free to code your own
patterns!

* :meth:`cosmos.util.tool.one2one`
* :meth:`cosmos.util.tool.many2one`
* :meth:`cosmos.util.tool.one2many`
* :meth:`cosmos.util.tool.many2many`



Notes
+++++++

    The above is **not** exhaustive.  For example, you could have a task who has 3 different parents, each belonging to a different stage.
    It is **highly** recommended that you get familiar with itertools, especially :py:func:`itertools.groupby`.  You will often want to group parent Tasks
    by a particular set of tags.



API
-----------

Execution.add
++++++++++++++

.. automethod:: cosmos.Execution.add


Inputs
++++++++++

.. automodule:: cosmos.models.Tool
    :members: Input, Inputs


Helpers
+++++++++++

.. automodule:: cosmos.util.tool
    :members: one2one, many2one, one2many, many2many