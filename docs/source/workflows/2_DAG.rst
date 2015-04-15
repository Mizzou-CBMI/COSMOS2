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

    stage_B = execution.add(Tool_One(input_task, tags=input_task.tags, parents=[input_task], out='tool_one_output/'))
                            for input_task in load_tasks)


One2many
++++++++
For each parent task in StageA, two or more new children are generated in StageB

Many2one
+++++++++
Two or more parents in StageA produce one task in StageB.

Many2many
+++++++++
Two or more parents in StageA produce two or more parents in StageB.

The above is **not** exhaustive.  For example, you could have a task who has 3 different parents, each belonging to a different stage.
It is **highly** recommended that you get familiar with itertools, especially :py:func:`itertools.groupby`.  You will often want to group parent Tasks
by a particular set of tags.  See :ref:`examples`.


API
-----------

.. automodule:: cosmos.Execution
    :members: add


.. automodule:: cosmos.models.Tool
    :members: Input, Inputs
