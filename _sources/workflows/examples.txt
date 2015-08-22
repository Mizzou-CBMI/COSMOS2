.. _examples:

Workflow Code Examples
======================

The easiest way to learn is often by example.  When you're learning to write your own workflows,
make liberal use of the :py:meth:`~cosmos.flow.dag.DAG.create_dag_img` and the `visualize` button
in the runweb interface.

Basic Workflows
++++++++++++++++

Hello World
___________

Here is the source code of the :file:`example_workflows/ex1.py` you ran in :ref:`getting_started`.

.. literalinclude:: ../../../examples/ex1.py

Here's the :term:`DAG` that was created.  If you use the web-interface, there will the nodes are convenient links
you can click to jump straight to the debug information that pertains to that Task or Stage.

.. figure:: /_static/imgs/ex1_stage_graph.png
    :align: center

The Stage Graph is a high level overview.  Often the :term:`DAG` of Tasks
is too large for a visualization to be useful:

.. figure:: /_static/imgs/ex1_task_graph.png
    :align: center


Tool Definitions used by Examples
------------------------------------

.. literalinclude:: ../../../examples/tools.py