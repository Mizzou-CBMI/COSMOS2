Shell
=============

The Cosmos shell is an interactive IPython session with useful modules already imported.  It is a very powerful way to explore, manage, administrate and debug
your workflows.  It is highly recommended to interactively explore your workflows using the wonderful `IPython Notebook <http://ipython.org/notebook.html>`_

As an example, **wf** will be the most recent Workflow when you launch into the shell.

To launch the shell, just run:

.. code-block:: text

    $ cosmos shell /path/to/database.sqlite
    or
    $ cosmos shell sqlalchemy_database_url

    Python 2.7.12 (default, Jul 21 2016, 20:22:53)
    Type "copyright", "credits" or "license" for more information.

    IPython 3.2.3 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: wf.stages
    Out[1]:
    [<Stage[1] Hello>,
     <Stage[2] World>]

.. note::

    The list of the `workflows` list will become stale if another process runs a new Workflow.  Either restart the shell, or re-run
    the :term:`SQLALchemy` query:

    .. code-block:: python

        >>> workflows = list(session.query(Workflow).all())



Delete a Stage and all of it's Descendants
------------------------------------------
When you're developing workflows, things inevitably will go wrong.  More often than not, it is useful to fix a particular Stage and restart the Workflow
from there.  This avoids a lot of unnecessary re-computation of Stages that weren't affected by your code fix.

.. code-block:: python

    >>> wf.stages[4].delete(descendants=True)
    # or
    >>> for stage in wf.stages[4:]: stage.delete()
    # or
    >>> wf.stages[2].task.delete(descendants=True)

Note that setting delete_files=True can be slow if there are a lot of files to delete.  Usually it's better (especially in development) to set
delete_files=False and just have the next run overwrite the files.


Getting a Stage or Tasks Descendants
---------------------------------------

.. code-block:: python

    >>> wf.stages[4].descendants(include_self=True)
    >>> wf.stages[3].tasks[0].descendants(include_self=True)


Manually Altering Attributes
-------------------------------

.. code-block:: python

    >>> wf.name='My_New_Workflow_Name'
    >>> wf.stages[0].name='My_New_Stage_Name'
    >>> for wf in workflows[-6:]: wf.status = WorkflowStatus.successful
    >>> session.commit() # write all changes to database so that they persist.  Always do this after you're done modifying objects.

API
-----------

.. automethod:: cosmos.api.Cosmos.shell
