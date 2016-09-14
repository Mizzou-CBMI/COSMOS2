Advanced Functionality
=======================

Using signals
++++++++++++++



.. code-block:: python

    import os
    import signal
    from cosmos.api import TaskStatus, signal_task_status_change

    @signal_task_status_change.connect
    def task_status_changed(task):
        if task.status in [TaskStatus.successful, TaskStatus.failed]:
            if task.stage.name == 'MyStage':
                with open(task.output_map['out_file']) as fp:
                    if any('ERROR' in line for line in fp):
                        # Send a ctrl+c signal to the main process, which will cause it to clean-up and terminate
                        os.kill(os.getpid(), signal.SIGINT)


