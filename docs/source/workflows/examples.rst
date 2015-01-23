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

Here's the job dependency graph that was created:

.. figure:: ../imgs/ex1.png
    :width: 100%
    :align: center