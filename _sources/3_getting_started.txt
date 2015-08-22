.. _getting_started:

Getting Started
===============

We'll start by running a simple test workflow, exploring it via the web interface, and terminating it.  Then
you'll be ready to start learning how to write your own.

Execute an Example Workflow
___________________________

The console will generate a lot of output as the workflow runs.  This workflow tests out various
features of Cosmos.  The number beside each object inside brackets, `[#]`, is the SQL ID of that object.  Clone the
git repository so that you have access to the examples code:

.. code-block:: bash

    $ git clone https://github.com/LPM-HMS/Cosmos2 Cosmos
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
    Initializing db...
    
    $ python main.py -h
    usage: main.py [-h] <command> ...

    optional arguments:
      -h, --help  show this help message and exit

    Commands:
      <command>
        resetdb   Resets the database. This is not reversible!
        initdb    Initialize the database via sql CREATE statements
        shell     Launch an IPython shell with useful variables already imported
        runweb
        ex1       Example1
        ex2       Example2: A failed task
        ex3       Example3: Twilio SMS (note you must edit the file)

    $ python main.py ex1 -h
    usage: main.py ex1 [-h] -n NAME [-o OUTPUT_DIR] [-c MAX_CPUS]
                   [-m MAX_ATTEMPTS] [-r] [-y]

    optional arguments:
      -h, --help            show this help message and exit
      -n NAME, --name NAME  A name for this execution
      -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                            The directory to output files to. Path should not
                            exist if this is a new execution.
      -c MAX_CPUS, --max_cpus MAX_CPUS
                            Maximum number (based on the sum of cpu_requirement)
                            of cores to use at once. 0 means unlimited
      -m MAX_ATTEMPTS, --max_attempts MAX_ATTEMPTS
                            Maximum number of times to try running a Task that
                            must succeed before the execution fails
      -r, --restart         Completely restart the execution. Note this will
                            delete all record of the execution in the database
      -y, --skip_confirm    Do not use confirmation prompts before restarting or
                            deleting, and assume answer is always yes



Launch the Web Interface
________________________

You can use the web interface to explore the history and debug all workflows.  Features include:


* Visualizing all jobs as a dependency graph (not useful if there are too many jobs)
* Visualizing the stages as a dependency graph (high level overview)
* Search for particular tasks based on their tags or other attributes
* See resource usage statistics
* For any task, view the exact command that was executed, stdout, stderr, resource usage, inputs/outputs, dependencies, etc.

.. code-block:: bash

   python examples/main.py runweb --host 0.0.0.0 --port 8080

Visit `<http://servername:8080>`_ to access it (or`<http://localhost:8080>`_ if you're running cosmos locally.


.. figure:: /_static/imgs/web_interface.png
   :align: center

.. hint::

    If the cosmos webserver is running, but you can't connect, it is likely because there is a firewall
    in front of the server.  You can get around it by using **ssh port forwarding**, for example"
    `$ ssh -L 8080:servername:8080 user@server`.  And if that fails, the Cosmos web interface works well
    using lynx.

.. warning::

    The webserver is **NOT** secure.  If you need it secured, you'll have to set it up in a production
    Flask web server environment, see `<Deploying Flask http://flask.pocoo.org/docs/0.10/deploying/>`_.

Terminating a Workflow
______________________

To terminate a workflow, simply press ctrl+c (or send the process a SIGINT signal) in the terminal.
Cosmos will terminate running jobs and mark them as failed.
You can resume from the point in the workflow you left off later.

Resuming a workflow
____________________

A workflow can be resumed by re-running the script that originally started it.  Usually that means just re-running any
failed tasks.  However, it is a bit more complicated if you modify the script, or manually delete successful jobs. The
algorithm for resuming is as follows:

1) Delete any failed Tasks

* output_files are not cleaned up, it is expected they will be over-written

2) Add any new Tasks

* A Task is "new" if a Task with the same stage and set of tags does not exist.

3) Run the workflow

* Successful tasks will not be re-run.  Only new tasks added in 2) will be re-run.

.. warning::
    If a task in a stage with the same tags and has already been executed successfully, it
    will not be re-executed or altered, *even if the actual command has changed because
    you modified the script*.  If you look at the algorithm above, the successful task was never deleted in 1), so it
    did not get added in 2).  In the future Cosmos may emmit a warning when this occurs.
    This can be especially tricky when you try to change a successful task that has no tags.