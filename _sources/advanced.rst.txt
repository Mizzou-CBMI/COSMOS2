Advanced
=======================


args_to_str
+++++++++++++
This is a useful helper function for converting Task function arguments to command line arguments.

.. code-block:: python

    from cosmos.api import args_to_str

    def foo(in_tsv, out_tsv, bar, x=None):
        return "foo {in_tsv} {out_tsv} {args}".format(
            args=args_to_str(('--bar', bar), ('--x', x)), **locals()
        )


.. autofunction:: cosmos.api.args_to_str


Bash Call
++++++++++

Tired of writing argparse boiler plate code for every Task you wrote a python script for?
You can use this decorator to avoid writing both argparse code, and avoid writing the Task function that returns
the bash command to call your script.  Notice how in this example, the code in :func:`stats_summary` is not returning a string which then calls another script.
The :func:`cosmos.api.bash_call` decorator does that for you automatically!  It's a bit to wrap your head around at first, but once it clicks you'll love it.


.. code-block:: python

    def stats_summary(in_tsv, out_tsv):
        count = 0
        with open(in_tsv) as fp:
            for line in fp:
                count += 1
        with open(out_tsv, 'w') as fp:
            print >> fp, count

    workflow.add_task(bash_call(stats_summary), params=dict(in_tsv='in_tsv',out_tsv='out_tsv'), uid='')

.. autofunction:: cosmos.api.bash_call



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

