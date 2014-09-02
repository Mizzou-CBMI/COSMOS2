Getting Started
===============

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

    $ cd Cosmos/examples
    $ python ex1.py

    INFO: 2014-09-02 19:30:48: Rendering taskgraph for <Execution[1] test> using DRM `local`, output_dir: `/locus/home/egafni/projects/Cosmos/examples/out/test`
    INFO: 2014-09-02 19:30:48: Committing 10 Tasks to the SQL database...
    INFO: 2014-09-02 19:30:48: <Stage[1] Echo> Has not been attempted
    INFO: 2014-09-02 19:30:48: <Stage[2] Cat> Has not been attempted
    INFO: 2014-09-02 19:30:48: <Stage[3] WordCount> Has not been attempted
    INFO: 2014-09-02 19:30:48: Skipping 0 successful tasks
    INFO: 2014-09-02 19:30:48: Executing TaskGraph
    INFO: 2014-09-02 19:30:48: <Stage[1] Echo> Running
    INFO: 2014-09-02 19:30:48: <Task[1] Echo {'word': 'hello'}> Submitted to the job manager. drm=local; drm_jobid=15911
    INFO: 2014-09-02 19:30:48: <Task[10] Echo {'word': 'world'}> Submitted to the job manager. drm=local; drm_jobid=15921
    INFO: 2014-09-02 19:30:49: <Task[1] Echo {'word': 'hello'}> Finished successfully
    INFO: 2014-09-02 19:30:50: <Stage[2] Cat> Running
    INFO: 2014-09-02 19:30:50: <Task[2] Cat {'word': 'hello', 'n': 1}> Submitted to the job manager. drm=local; drm_jobid=15931
    INFO: 2014-09-02 19:30:50: <Task[3] Cat {'word': 'hello', 'n': 2}> Submitted to the job manager. drm=local; drm_jobid=15942
    INFO: 2014-09-02 19:30:50: <Task[10] Echo {'word': 'world'}> Finished successfully
    INFO: 2014-09-02 19:30:50: <Stage[1] Echo> Finished successfully
    INFO: 2014-09-02 19:30:50: <Task[8] Cat {'word': 'world', 'n': 1}> Submitted to the job manager. drm=local; drm_jobid=15953
    INFO: 2014-09-02 19:30:50: <Task[9] Cat {'word': 'world', 'n': 2}> Submitted to the job manager. drm=local; drm_jobid=15961
    INFO: 2014-09-02 19:30:51: <Task[2] Cat {'word': 'hello', 'n': 1}> Finished successfully
    INFO: 2014-09-02 19:30:51: <Stage[3] WordCount> Running
    INFO: 2014-09-02 19:30:51: <Task[5] WordCount {'word': 'hello', 'n': 1}> Submitted to the job manager. drm=local; drm_jobid=15975
    INFO: 2014-09-02 19:30:51: <Task[3] Cat {'word': 'hello', 'n': 2}> Finished successfully
    INFO: 2014-09-02 19:30:51: <Task[4] WordCount {'word': 'hello', 'n': 2}> Submitted to the job manager. drm=local; drm_jobid=15986
    INFO: 2014-09-02 19:30:51: <Task[8] Cat {'word': 'world', 'n': 1}> Finished successfully
    INFO: 2014-09-02 19:30:51: <Task[9] Cat {'word': 'world', 'n': 2}> Finished successfully
    INFO: 2014-09-02 19:30:51: <Stage[2] Cat> Finished successfully
    INFO: 2014-09-02 19:30:51: <Task[7] WordCount {'word': 'world', 'n': 2}> Submitted to the job manager. drm=local; drm_jobid=15997
    INFO: 2014-09-02 19:30:51: <Task[6] WordCount {'word': 'world', 'n': 1}> Submitted to the job manager. drm=local; drm_jobid=16005
    INFO: 2014-09-02 19:30:52: <Task[5] WordCount {'word': 'hello', 'n': 1}> Finished successfully
    INFO: 2014-09-02 19:30:52: <Task[4] WordCount {'word': 'hello', 'n': 2}> Finished successfully
    INFO: 2014-09-02 19:30:52: <Task[7] WordCount {'word': 'world', 'n': 2}> Finished successfully
    INFO: 2014-09-02 19:30:52: <Task[6] WordCount {'word': 'world', 'n': 1}> Finished successfully
    INFO: 2014-09-02 19:30:52: <Stage[3] WordCount> Finished successfully
    INFO: 2014-09-02 19:30:52: <Execution[1] test> Finished successfully, output_dir: /locus/home/egafni/projects/Cosmos/examples/out/test

Organizing your project
________________________

see examples/main.py for a way to organize multiple workflows into a single access point.

.. code-block:: bash

    $ python main.py initdb
    $ python main.py -h
    $ python main.py ex1 -h




Launch the Web Interface
________________________

You can use the web interface to explore the history and debug all workflows.  To start it, run:

.. code-block:: bash

   python examples/runweb.py

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
    especially tricky when you try to change a successful task that has no tags.