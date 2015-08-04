.. _examples:

Workflow Code Examples
======================

The easiest way to learn is often by example.  When you're learning to write your own workflows,
make liberal use of the :py:meth:`~cosmos.lib.ezflow.dag.DAG.create_dag_img` and the `visualize` button
in the web interface.

Basic Workflows
++++++++++++++++

Hello World
___________

Here is the source code of the :file:`example_workflows/ex1.py` you ran in :ref:`getting_started`.

.. literalinclude:: ../../../example_workflows/ex1.py

Here's the job dependency graph that was created:

.. figure:: ../imgs/ex1.png
    :width: 100%
    :align: center

Reload a Workflow
_________________

You can add more stages to the workflow, without re-running tasks that were already successful.
An example is in :file:`example_workflows/ex2.py`.

.. literalinclude:: ../../../example_workflows/ex2.py

Run it with the command:

.. code-block:: bash

   $ python ex1_b.py


Advanced Workflows
++++++++++++++++++

Branching Workflows
___________________

:file:`example_workflows/ex_branch.py`

.. literalinclude:: ../../../example_workflows/ex_branch.py

Command Line Interface and Signals
__________________________________

:file:`example_workflows/ex_signals.py`

.. literalinclude:: ../../../example_workflows/ex_signals.py

