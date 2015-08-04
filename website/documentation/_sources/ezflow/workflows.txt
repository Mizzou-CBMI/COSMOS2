.. _writing_workflows:

Designing Workflows
===================

All jobs and and job dependencies are represented by the :py:class:`DAG` class.

There are various **FlowFxns** you can use to generate a DAG.  The construction of a :py:class:`DAG` always starts with
a call to :py:meth:`~dag.DAG.add_`, which adds a list of tools with no dependencies.  The new tools that were add_()ed become
the :py:class:`DAG`'s `active_tools`, which are the list of tools that all other **FlowFxns** operate on.  Generally
speaking, the `active_tools` are the last tools that were added to the DAG by a **FlowFxns**, however, there are a few
tricks have more control over what is stored in `active_tools`.

Although you can chain the **FlowFxns** together like this: ``DAG().add_([tool1,tool2]).map_(MyTool)``, you are offered
much more flexibility to modularize workflow logic by using the object representations of the **FlowFxns**.

.. code-block:: python

    from cosmos.lib.ezflow.dag import sequence_, apply_, map_, dag_
    dag = DAG()

    subworkflow1 = map_(ToolA).split_([('color',['red','blue'])],ToolB)
    subworkflow2 = map_(Tool1).map_(Tool2).sequence_(subworkflow1)
    dag._sequence(subworkflow2)

.. hint::

    You can always visualize the ``DAG`` that you've built using :py:meth:`dag.DAG.create_dag_img`.
    (see :ref:`examples` for details)


.. seealso::
    There are a lot of useful examples.  Some of the more advanced examples even demonstrate
    useful advanced features that are not described here yet.
    :ref:`examples`


TheFlowFxn Operators
---------------------

Below are the FlowFxn operators.  It is recommended that you take a look at the :ref:`examples` to get an understanding
of how they work.

.. automethod:: cosmos.lib.ezflow.dag.DAG.add_
.. automethod:: cosmos.lib.ezflow.dag.DAG.map_
.. automethod:: cosmos.lib.ezflow.dag.DAG.reduce_
.. automethod:: cosmos.lib.ezflow.dag.DAG.split_
.. automethod:: cosmos.lib.ezflow.dag.DAG.reduce_split_
.. automethod:: cosmos.lib.ezflow.dag.DAG.sequence_
.. automethod:: cosmos.lib.ezflow.dag.DAG.apply_
.. automethod:: cosmos.lib.ezflow.dag.DAG.branch_


API
-----------

DAG
****

.. automodule:: cosmos.lib.ezflow.dag
    :private-members:
    :members:
    :undoc-members:
