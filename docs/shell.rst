The COSMOS IPython shell provides a powerful way to inspect, alter and debug your workflows.  The code is very simple, see `meth`:cosmos.Cosmos.shell()

.. code-block:: python
    # set this to an example
    from cosmos import Cosmos
    app = Cosmos():
    app.shell()

.. code-block:: bash

    $ python examples/cosmos.py shell

    In [1]: executions
    Out[1]:
    [<Execution[1] Simple>,
     <Execution[2] Fail>]

    # (this most recent execution)
    In [2]: print ex
    Out[2]: <Execution[13] Fail>

    # delete all descendants of the second to last stage (children of children are deleted, etc).
    In [3]: for s in ex.stages[-2].descendants(include_self=True): s.delete(delete_files=True)
    INFO: 2014-06-27 17:34:05: Deleting <Stage[4] ...>
    ...

    In [4]: # (ctrl+d to exit)
    Do you really want to exit ([y]/n)? y