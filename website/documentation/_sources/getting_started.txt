.. _getting_started:

Getting Started
===============

We'll start by running a simple test workflow, exploring it via the web interface, and terminating it.  Then
you'll be ready to start learning how to write your own.

Execute an Example Workflow
___________________________
   
The console will generate a lot of output as the workflow runs.  This workflow tests out various
features of Cosmos.  The number beside each object inside brackets, `[#]`, is the ID of that object.

.. code-block:: bash

   $ cd Cosmos
   $ python example_workflows/ex1.py
   
   wrote to /tmp/ex1.svg
   INFO: 2012-12-06 16:45:51: Created Workflow Workflow[2] Example 1.
   INFO: 2012-12-06 16:45:51: Adding tasks to workflow.
   INFO: 2012-12-06 16:45:51: Creating Stage[16] ECHO from scratch.
   INFO: 2012-12-06 16:45:51: Creating Stage[17] CAT from scratch.
   INFO: 2012-12-06 16:45:51: Total tasks: 6, New tasks being added: 6
   INFO: 2012-12-06 16:45:51: Bulk adding 6 TaskFiles...
   INFO: 2012-12-06 16:45:51: Bulk adding 6 Tasks...
   INFO: 2012-12-06 16:45:51: Bulk adding 10 TaskTags...
   INFO: 2012-12-06 16:45:51: Bulk adding 4 task edges...
   INFO: 2012-12-06 16:45:51: Generating DAG...
   INFO: 2012-12-06 16:45:51: Running DAG.
   INFO: 2012-12-06 16:45:51: Running Task[91] ECHO {'word': 'hello'}
   INFO: 2012-12-06 16:45:51: Submitted jobAttempt with drmaa jobid 4039.
   INFO: 2012-12-06 16:45:52: Running Task[94] ECHO {'word': 'world'}
   Job <4040> is submitted to default queue <medium_priority>.
   INFO: 2012-12-06 16:45:52: Submitted jobAttempt with drmaa jobid 4040.
   INFO: 2012-12-06 16:46:00: Task[91] ECHO {'word': 'hello'} Successful!
   INFO: 2012-12-06 16:46:00: Running Task[92] CAT {'i': 1, 'word': 'hello'}
   INFO: 2012-12-06 16:46:00: Submitted jobAttempt with drmaa jobid 4041.
   INFO: 2012-12-06 16:46:00: Running Task[93] CAT {'i': 2, 'word': 'hello'}
   INFO: 2012-12-06 16:46:00: Submitted jobAttempt with drmaa jobid 4042.
   INFO: 2012-12-06 16:46:01: Task[94] ECHO {'word': 'world'} Successful!
   INFO: 2012-12-06 16:46:01: Stage Stage[16] ECHO successful!
   INFO: 2012-12-06 16:46:01: Running Task[95] CAT {'i': 1, 'word': 'world'}
   INFO: 2012-12-06 16:46:01: Submitted jobAttempt with drmaa jobid 4043.
   INFO: 2012-12-06 16:46:01: Running Task[96] CAT {'i': 2, 'word': 'world'}
   INFO: 2012-12-06 16:46:01: Submitted jobAttempt with drmaa jobid 4044.
   INFO: 2012-12-06 16:46:12: Task[93] CAT {'i': 2, 'word': 'hello'} Successful!
   INFO: 2012-12-06 16:46:12: Task[92] CAT {'i': 1, 'word': 'hello'} Successful!
   INFO: 2012-12-06 16:46:13: Task[96] CAT {'i': 2, 'word': 'world'} Successful!
   INFO: 2012-12-06 16:46:14: Task[95] CAT {'i': 1, 'word': 'world'} Successful!
   INFO: 2012-12-06 16:46:14: Stage Stage[17] CAT successful!
   INFO: 2012-12-06 16:46:14: Finished.

Launch the Web Interface
________________________

You can use the web interface to explore the history and debug all workflows.  To start it, run:

.. code-block:: bash

   cosmos runweb -p 8080
  
.. note::

    Currently the system you're running the web interface on must be the same (or have :term:`DRMAA` access to) as the
    system you're running the workflow on.
   
Visit `<http://servername:8080>`_ to access it (or`<http://localhost:8080>`_ if you're running cosmos locally.


.. figure:: /imgs/web_interface.png
   :width: 90%
   :align: center

.. hint::

    If the cosmos webserver is running, but you can't connect, it is likely because there is a firewall
    in front of the server.  You can get around it by using **ssh port forwarding**, for example"
    `$ ssh -L 8080:servername:8080 user@server`.  And if that fails, the Cosmos web interface works very well
    using lynx.

.. warning::

    The webserver is **NOT** secure.  If you need it secured, you'll have to set it up in a production
    Django web server environment (for example, using **mod_wsgi** with **Apache2**).

Terminating a Workflow
______________________

To terminate a workflow, simply press ctrl+c (or send the process a SIGINT signal) in the terminal.
Cosmos will terminate running jobs and mark them as failed.
You can resume from the point in the workflow you left off later.

Resuming a workflow
____________________

A workflow can be resumed by re-running a script that originally.  The algorithm for resuming is as follows:

1) Delete any failed tasks
2) Add any tasks that do not exist in the database (Keyed be the task's stage name and tags)
3) Run the workflow

.. warning::
    If a task in a stage with the same tags has already been executed successfully, it
    will not be re-executed or altered, *even if the actual command has changed because
    you modified the script*.  In the future Cosmos may emmit a warning when this occurs or automatically
    re-run these tasks.  This can be
    especially tricky when you try to change task that has no tags (this can happen when it's the only task
    in it's stage), and has executed successfully.