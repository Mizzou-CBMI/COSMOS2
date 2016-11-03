.. _examples:

Examples
======================

The easiest way to learn is often by example.  When you're learning to write your own workflows,
make liberal use of the :py:meth:`~cosmos.flow.dag.DAG.create_dag_img` and the `visualize` button
in the runweb interface.  More examples are available in the examples/ directory of the github repository.

It might look verbose, but that's because the Cosmos api is very *explicit*.  There's no DSL to string together your pipeline
like other workflow managers - we've found that works most of the time; the other 20% of the time it is a huge headache.


Example 1
--------------

:file:`examples/ex1.py`

.. literalinclude:: ../../examples/ex1.py

Running the above file...

.. code-block:: bash

    17:29 $ python ex1.py
    Initializing sql database for Cosmos v2.0.11...
    INFO: 2016-11-02 17:29:53: Deleting <Workflow[1] Example1>, delete_files=False
    <Workflow[1] Example1> Deleting from SQL...
    <Workflow[1] Example1> Deleted
    INFO: 2016-11-02 17:29:53: Execution Command: ex1.py
    task.params {'core_req': 2, 'text': 'Hello World', 'out_file': 'out.txt'}
    task.input_map {}
    task.output_map {'out_file': 'out.txt'}
    task.core_req 2
    task.drm local
    task.uid my_task
    INFO: 2016-11-02 17:29:53: Preparing to run <Workflow[1] Example1> using DRM `local`, cwd is `/Users/egafni/projects/Cosmos/examples/analysis_output/ex1`
    INFO: 2016-11-02 17:29:53: <Stage[6] Say> Has not been attempted
    INFO: 2016-11-02 17:29:53: Skipping 0 successful tasks...
    INFO: 2016-11-02 17:29:53: Committing to SQL db...
    INFO: 2016-11-02 17:29:53: Executing TaskGraph
    INFO: 2016-11-02 17:29:54: <Stage[6] Say> Running (stage has 0/1 successful tasks)
    INFO: 2016-11-02 17:29:54: <Task[13] Say(uid='my_task')> Submitted to the job manager. drm=local; drm_jobid=54670
    INFO: 2016-11-02 17:29:54: <Task[13] Say(uid='my_task')> Finished successfully
    INFO: 2016-11-02 17:29:54: <Stage[6] Say> Finished successfully (stage has 1/1 successful tasks)
    INFO: 2016-11-02 17:29:54: <Workflow[1] Example1> Successfully Finished

Example 2
--------------

Here is the source code of the :file:`example_workflows/ex2.py` you ran in :ref:`getting_started`.

:file:`examples/ex2.py`

.. literalinclude:: ../../examples/ex2.py

Here's the :term:`DAG` that was created.  If you use the web-interface, the nodes are convenient links
you can click to jump straight to the debug information that pertains to that Task or Stage.

.. figure:: /_static/imgs/ex1_stage_graph.png
    :align: center

The Stage Graph is a high level overview.  Often the :term:`DAG` of Tasks
is too large for a visualization to be useful:

.. figure:: /_static/imgs/ex1_task_graph.png
    :align: center

Tools
++++++
Tools used by Example2

:file:`examples/tools.py`

.. literalinclude:: ../../examples/tools.py