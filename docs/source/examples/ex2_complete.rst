.. _example2:


Complete Example
--------------

Here is the source code of the :file:`examples/ex2_complete.py` you ran in :ref:`getting_started`.

:file:`examples/ex2.py`

.. literalinclude:: ../../../examples/ex2_complete.py

Here's the :term:`DAG` that was created.  If you use the web-interface, the nodes are convenient links
you can click to jump straight to the debug information that pertains to that Task or Stage.

.. figure:: /_static/imgs/ex1_stage_graph.png
    :align: center

The Stage Graph is a high level overview.  Often the :term:`DAG` of Tasks
is too large for a visualization to be useful:

.. figure:: /_static/imgs/ex1_task_graph.png
    :align: center