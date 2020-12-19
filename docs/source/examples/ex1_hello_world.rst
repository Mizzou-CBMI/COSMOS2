.. _hello_world:


Hello World
--------------

:file:`examples/ex1.py`

.. literalinclude:: ../../../examples/ex1_hello_world.py

Running the above file...

.. code-block:: bash

    17:29 $ python ex1.py
    Initializing sql database for Cosmos v2.0.11...
    INFO: 2016-11-02 17:29:53: Deleting <Workflow[1] Example1>, delete_files=False
    <Workflow[1] Example1> Deleting from SQL...
    <Workflow[1] Example1> Deleted
    INFO: 2016-11-02 17:29:53: Execution Command: ex1.py
    task.params {'core_req': 2, 'text': 'Hello World', 'out_file': 'out.txt'}
    task.input_map {}
    task.output_map {'out_file': 'out.txt'}
    task.core_req 2
    task.drm local
    task.uid my_task
    INFO: 2016-11-02 17:29:53: Preparing to run <Workflow[1] Example1> using DRM `local`, cwd is `/Users/egafni/projects/Cosmos/examples/analysis_output/ex1`
    INFO: 2016-11-02 17:29:53: <Stage[6] Say> Has not been attempted
    INFO: 2016-11-02 17:29:53: Skipping 0 successful tasks...
    INFO: 2016-11-02 17:29:53: Committing to SQL db...
    INFO: 2016-11-02 17:29:53: Executing TaskGraph
    INFO: 2016-11-02 17:29:54: <Stage[6] Say> Running (stage has 0/1 successful tasks)
    INFO: 2016-11-02 17:29:54: <Task[13] Say(uid='my_task')> Submitted to the job manager. drm=local; drm_jobid=54670
    INFO: 2016-11-02 17:29:54: <Task[13] Say(uid='my_task')> Finished successfully
    INFO: 2016-11-02 17:29:54: <Stage[6] Say> Finished successfully (stage has 1/1 successful tasks)
    INFO: 2016-11-02 17:29:54: <Workflow[1] Example1> Successfully Finished
