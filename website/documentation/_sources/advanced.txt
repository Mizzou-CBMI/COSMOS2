.. _advanced:

Advanced Usage
==============

Setting Custom Job Submission Flags
+++++++++++++++++++++++++++++++++++

If you want to specify custom :term:`DRMS` specific flags, all you have to do is set
:py:data:`cosmos.session.get_drmaa_native_specification` in your workflow script.

.. hint::

    By default, cosmos uses :py:meth:`cosmos.session.default_get_drmaa_native_specification` and you'll probably
    want to take a look at its source code.


For example, to submit to a queue depending on the task's time_requirement:

.. code-block:: python

    from cosmos import session

    def my_get_drmaa_native_specification(jobAttempt):
    task = jobAttempt.task
    DRM = settings['DRM']

    cpu_req = task.cpu_requirement
    mem_req = task.memory_requirement
    time_req = task.time_requirement
    queue = task.workflow.default_queue

    if time_req < 10:
        queue = 'mini'
    if time_req < 12*60:
        queue = 'short'
    else:
        queue = 'i2b2_unlimited'

    if DRM == 'LSF':
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req,cpu_req)
        if time_req:
            s += ' -W 0:{0}'.format(time_req)
        if queue:
            s += ' -q {0}'.format(queue)
        return s
    else:
        raise Exception('DRM not supported')

    session.get_drmaa_native_specification = get_drmaa_native_specification

API
-----------

Session
********
.. automodule:: cosmos.session
    :members:

