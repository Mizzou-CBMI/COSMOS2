.. _shell:

Cosmos Shell
============

Using various Django features and apps, you can quickly enter an ipython shell with access to all your workflow objects.

.. note:: This is an advanced feature, and relies on `Django Queries <https://docs.djangoproject.com/en/dev/topics/db/queries/>`_

Launch the IPython shell
++++++++++++++++++++++++

.. code-block:: bash

   $ cosmos shell
 
You can then interactively investigate and perform all sorts of operations.
This is really powerful when interacting with the
:term:`Django` API, since most of the Cosmos objects are Django models.
Most Cosmos classes are automatically imported for you.

.. code-block:: python 

   all_workflows = Workflow.objects.all()
   workflow = all_workflows[2]
   stage = workflow.stages[3]
   stage.file_size
   stage.tasks[3].get_successful_jobAttempt().queue_status
   

Interactive Workflow
++++++++++++++++++++

You can even run a workflow:

.. code-block:: python 

    wf = Workflow.start('Interactive')
    stage = wf.add_stage('My Stage')
    task = stage.add_task('echo "hi"')
    wf.run()
    wf.finished()

Filtering
++++++++++

pk is a shortcut for primary key, so you can use this to quickly get objects you're seeing in the web interface,
which is always displayed.  For example, if you see "Stage[200] Stage Name", you can query for it like so

.. code-block:: python

    Workflow.objects.get(pk=200)

Or for multiple objects:

.. code-block:: python

    Workflow.objects.filter(pk__in=[200,201,300])

You can also query on the many fields available for each object

.. code-block:: python

    Task.objects.filter(status='in_progress',memory_requirement=1024)

For filtering by tags, use the special method :py:meth:`cosmos.Workflow.models.Workflow.get_task_by`.

.. code-block:: python

    wf = Workflow.objects.get(name="My Workflow")
    wf.get_task_by(tags={'color':'orange','shape':'circle'})

For more advanced queries, see `Django Queries <https://docs.djangoproject.com/en/dev/topics/db/queries/>`_.

Deleting
+++++++++

You can delete records by simply calling `object.delete()`

.. warning::

    Do not call .delete() on a queryset, as it will not run a lot of important cleanup code.  i.e. don't do this:

    >>> Task.objects.get(success=False).delete()
    or
    >>> Stage.objects.get(name="My Stage").delete()

    Instead, do the following, which will perform a lot of extra important code for each task:

    >>> for t in Task.objects.get(success=False): t.delete()
    or
    >>> for s in Stage.objects.get(name="My Stage"): s .delete()

