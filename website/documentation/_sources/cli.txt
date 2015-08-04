.. _cli:

Command Line Interface
======================

Make sure your environment variables are properly set (see :ref:`config`).
Use the shell command :command:`env` if you're not sure what's in your environment.

.. code-block:: bash

    $ cosmos -h
    usage: cosmos [-h] <command> ...

    Cosmos CLI

    optional arguments:
      -h, --help  show this help message and exit

    Commands:
      <command>
        resetdb   DELETE ALL DATA in the database and then run a syncdb
        shell     Open up an ipython shell with Cosmos objects preloaded
        syncdb    Sets up the SQL database
        list      List all workflows
        runweb    Start the webserver

         
Explore the available commands, using -h if you wish.  Or see the :ref:`cli` for more info.  Note that when
listing workflows, the number beside each Workflow inside brackets, `[#]`, is the ID of that object.

Examples
________

Get Usage Help:
+++++++++++++++
.. code-block:: bash

   $ cosmos -h
   
Reset the SQL database
++++++++++++++++++++++
.. warning:: This will *not* delete the files associated with workflow output.

.. code-block:: bash

   $ cosmos resetdb

List workflows
++++++++++++++
.. code-block:: bash

   $ cosmos ls
