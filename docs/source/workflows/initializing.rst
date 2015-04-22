.. _tools:

Initializing
===============

Cosmos is initialized using the :class:`cosmos.Cosmos` object, which is subsequently used to starts instances of :class:`Execution`
which contain the entire structure of a workflow.

.. code-block:: python

    from cosmos import Cosmos
    cosmos = Cosmos('sqlite:///my_cosmos_db.sqlite')
    cosmos.initdb
    execution = cosmos.start('My_Workflow)



API
-----------

Cosmos
_______


.. autoclass:: cosmos.Cosmos
    :members: __init__, initdb, resetdb, runweb

.. autofunction:: cosmos.default_get_submit_args