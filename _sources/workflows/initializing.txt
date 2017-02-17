.. _initializing:

Initializing
===============

Cosmos is initialized by instantiating a :class:`cosmos.Cosmos` instance, which represents a connection to a SQL database.  The Cosmos instance can then
be used to start a Workflow.  If a Workflow already exists with the same name, it will be resumed and all failed Tasks will be deleted from the database.


.. code-block:: python

    from cosmos import Cosmos
    cosmos = Cosmos(database_url='sqlite:///my_cosmos_db.sqlite')
    cosmos.initdb() # creates the tables, if they already exist this does nothing
    workflow = cosmos.start(name='My_Workflow')

You may have to install the python package required to use the driver you'd like. For example, if you are using
``postgres+psycopg2:///``, make sure you ``pip install psycopg2``.  See `SQLAlchemy Engines <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_
for more details.

.. note::

    If is often very useful to maintain a one SQLite databsae per Workflow (especially for production environment), stored in the output directory of that Workflow.
    This way, Workflows are completely atomic and all their provenance is nicely packed in a single location.

API
-----------

Cosmos
_______


.. autoclass:: cosmos.api.Cosmos
    :members: __init__, initdb, start, resetdb, runweb

.. autofunction:: cosmos.api.default_get_submit_args
