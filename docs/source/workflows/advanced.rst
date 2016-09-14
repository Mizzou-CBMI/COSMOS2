Advanced Functionality
=======================

Using signals
++++++++++++++

COSMOS uses `blinked <https://pythonhosted.org/blinker/>`_ for signals.

.. code-block:: python

    from cosmos.api import TaskStatus, signal_task_status_change

    @signal_task_status_change.connect
    def task_status_changed(task):
        if task.status in [TaskStatus.successful, TaskStatus.failed]:
            if task.stage.name == 'MyStage':
                with open(task.output_map['out_file']) as fp:
                    if any('ERROR' in line for line in fp):
                        task.workflow.terminate()


