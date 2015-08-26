.. _recipes:


Executions
=================================

Executions consist of a :term:`DAG` of Tasks.  Tasks are bundled into Stages, but Stages have almost no functionality
and are mostly just for keeping track of similar Tasks.  Tasks execute as soon as their dependencies have completed.

Every task as a set of tags (a python `dict`), which serve two prinmary purposes:

* Uniquely identifies the tag.  All Tasks in a Stage have a unique set of tags.  When resuming an Execution,
* If you run :meth:`Execution.add_task` and a Task has already been successful with those tags, it will not be run again.  Think memoization.
* Passed as parameters to the `command_function`.  If a `command_function` does not have a default specified for one of its parameters, the tag will be required.

To create your :term:`DAG`, use :meth:`Execution.add_task`. Python generators
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
    execution = cosmos.start('My_Workflow', 'out_dir)

    # note in_txt is specified, so find() will not be used.
    wc_tasks = [ execution.add_task(word_count, tags=dict(in_txt='a.txt')) ]


Each call to :meth:`Execution.add_task` does the following:

1) Gets the corresponding Stage based on stage_name (which defaults to the name of of the `command_function`)
2) Checks to see if a Task with the same tags already completed successfully in that stage
3) If `2)` is True, then return that Task instance (it will also be skipped when the `DAG` is run)
4) if `2)` is False, then create and return new Task instance


Creating Your Job Dependency Graph (DAG)
---------------------------------------------------
A useful model for thinking about how your stages are related is to think in terms of SQL relationship types.

One2one (aka a map() operation)
+++++++++++++++++++++++++++++++
This is the most common stage dependency.  For each task in StageA, you create a single dependent task in StageB.


.. code-block:: python

    cosmos = Cosmos()
    cosmos.initdb()
    execution = cosmos.start('One2One', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add_task(tool_a, tags=dict(i=i))
                                 for i in [1, 2])
    stageB_tasks = execution.add_task(tool_b, ags=task.tags, parents=[task])
                                 for task in stageA_tasks)
    draw_task_graph(execution.task_graph(), 'one2one.png')

.. figure:: /_static/imgs/one2one.png
    :align: center


One2many
++++++++
For each parent task in StageA, two or more new children are generated in StageB

.. code-block:: python

    execution = cosmos.start('One2Many', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add_task(tool_a, tags=dict(i=i))
                                      for i in [1, 2])
    stageB_tasks = execution.add_task(tool_b, tags=dict(j=j, **task.tags), parents=[task])
                                      for task in stageA_tasks
                                      for j in ['a','b'])
    draw_task_graph(execution.task_graph(), 'one2many.png')


.. figure:: /_static/imgs/one2many.png
    :align: center



Many2one
++++++++++++++++++++++++++
Two or more parents in StageA produce one task in StageB.

.. code-block:: python

    execution = cosmos.start('Many2One', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add_task(tool_a, tags=dict(i=i, j=j))
                                      for i in [1, 2]
                                      for j in ['a','b'])
    get_i = lambda task: task.tags['i']
    stageB_tasks = execution.add_task(tool_b, tags=dict(i=i), parents=list(tasks))
                                      for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i))
    draw_task_graph(execution.task_graph(), 'many2one.png')


.. figure:: /_static/imgs/many2one.png
    :align: center

Many2many
+++++++++
Two or more parents in StageA produce two or more parents in StageB.

.. code-block:: python

    execution = cosmos.start('many2many', '/tmp', check_output_dir=False)
    stageA_tasks = execution.add_task(tool_a, tags=dict(i=i, j=j))
                                      for i in [1, 2]
                                      for j in ['a','b'])
    def B_generator(stageA_tasks):
        # For the more complicated relationships, it's usually best to just define a generator
        get_i = lambda task: task.tags['i']
        for i, tasks in it.groupby(sorted(stageA_tasks, key=get_i), get_i):
            parents = list(tasks)
            for k in ['x', 'y']:
                yield tool_b, tags=dict(i=i, k=k), parents=parents)

    stageB_tasks = execution.add_task(B_generator(stageA_tasks))
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

Execution
++++++++++++++

.. autoclass:: cosmos.Execution
    :members: add_task, run


Helpers
+++++++++++

.. automodule:: cosmos.util.tool
    :members: one2one, many2one, one2many, many2many