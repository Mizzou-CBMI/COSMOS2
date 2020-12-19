Advanced
=======================


Using signals
++++++++++++++

COSMOS uses `blinker <https://pythonhosted.org/blinker/>`_ for signals.

.. code-block:: python

    from cosmos.api import TaskStatus, signal_task_status_change

    @signal_task_status_change.connect
    def task_status_changed(task):
        if task.status in [TaskStatus.successful, TaskStatus.failed]:
            if task.stage.name == 'MyStage':
                with open(task.output_map['out_file']) as fp:
                    if any('ERROR' in line for line in fp):
                        task.workflow.terminate()


SGE Signals
+++++++++++++

A useful context manager is available to cleanly handle SGE signals provided by `qsub -notify`.

.. code-block:: python

    from cosmos.util.signal_handlers import SGESignalHandler, handle_sge_signals

    def main():
        handle_sge_signals()
        ...
        # create a dag and workflow, etc.
        ...
        with SGESignalHandler(workflow):
            workflow.run()

