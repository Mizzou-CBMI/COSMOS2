Cosmos Shell
=============

The Cosmos shell is an interactive python session with useful modules already imported.  It is a very powerful way to explore, manage, administrate and debug
your workflows.  It is highly recommended to interactively explore your workflows using the wonderful `IPython Notebook <http://ipython.org/notebook.html>`_


To launch the shell, create a script like this (Take a look at the :meth:`cosmos.api.Cosmos.shell` source code, it is very simple):

.. code-block:: python

    from cosmos import Cosmos
    cosmos_app = Cosmos('sqlite:///sqlite.db')
    cosmos.initdb()
    cosmos_app.shell()


.. note::

    The list of the `executions` list will become stale if another process runs a new Execution.  Either restart the shell, or re-run
    the :term:`SQLALchemy` query:

    .. code-block:: python

        >>> executions = list(session.query(Execution).all())



Delete a Stage and all of it's Descendants
------------------------------------------
When you're developing workflows, things inevitably will go wrong.  More often than not, it is useful to fix a particular Stage and restart the Execution
from there.  This avoids a lot of unnecessary re-computation of Stages that weren't affected by your code fix.

.. code-block:: python

    >>> ex.stages[4].delete(delete_files=True, delete_descendants=True)
    # or, if your DAG is simple
    >>> for stage in ex.stages[4:]: stage.delete()

Note that setting delete_files=True can be slow if there are a lot of files to delete.  Sometimes it's better (especially in development) to set
delete_files=False and just have the next run overwrite the files.


Getting a Stage's Descendants
------------------------------

.. code-block:: python

    >>> ex.stages[4].descendants(include_self=True)


Manually Altering Attributes
-------------------------------

.. code-block:: python

    >>> ex.name='My_New_Execution_Name'
    >>> ex.stages[0].name='My_New_Stage_Name'
    >>> for ex in executions[-6:]: ex.status = ExecutionStatus.successful
    >>> session.commit() # write all changes to database so that they persist.  Always do this after you're done modifying objects.

API
-----------

.. automethod:: cosmos.Cosmos.shell
