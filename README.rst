.. image:: https://travis-ci.org/LPM-HMS/COSMOS2.svg?branch=master
    :target: https://travis-ci.org/LPM-HMS/COSMOS2

For more information and the full documentation please visit
`http://lpm-hms.github.io/COSMOS2/ <http://lpm-hms.github.io/COSMOS2/>`_. 

To chat with the author/other users (many of which use Cosmos to make bioinformatics NGS workflows), use gitter:

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/LPM-HMS/COSMOS2
   :target: https://gitter.im/LPM-HMS/Cosmos2?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Install
==========

.. code-block:: python

    pip install Cosmos-wfm


Introduction
============
Cosmos is a python workflow management library primarily intended to process Big Data through scientific pipelines on a distributed computing cluster. 
It is used in production by large companies and researchers, and is popular in the bioinformatics community yet general enough for any distributed computing application.
 It allows you to efficiently program extremely complex workflows that are still readable and accessible to other software engineers,
 and provides a web dashboard to monitor, debug, and analyze your jobs.  Cosmos is
able to automatically process large amounts of data by utilizing traditional cluster such as LSF or GridEngine and a shared filesystem, which do not come
with the limitations of map reduce frameworks like Hadoop and Spark.  It is especially
powerful when combined with spot instances on `Amazon Web Services <aws.amazon.com>`_ and
`StarCluster <http://star.mit.edu/cluster/>`_ where we have processed hundreds of terrabytes in parallel in a single workflow.

Cosmos provides a simple but
flexible api to specify complex job DAGs, a way to resume modified or failed workflows, and make debugging and provenance as easy as possible.


History
___________

Cosmos was published as an Application Note in the journal `Bioinformatics <http://bioinformatics.oxfordjournals.org/>`_,
but has evolved a lot since it's original inception.  If you use Cosmos
for research, please cite it's `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/06/29/bioinformatics.btu385>`_. 

Since the original publication, it has been re-written and open-sourced by the original author, in a collaboration between
`The Lab for Personalized Medicine <http://lpm.hms.harvard.edu/>`_ at Harvard Medical School, the `Wall Lab <http://wall-lab.stanford.edu/>`_ at Stanford University, and
`Invitae <http://invitae.com>`_, a leading clinical genetic sequencing diagnostics laboratory.

Features
_________
* Written in python which is easy to learn, powerful, and popular.  A programmer with limited experience can begin writing Cosmos workflows right away.
* Powerful syntax for the creation of complex and highly parallelized workflows.
* Reusable recipes and definitions of tools and sub workflows allows for DRY code.
* Keeps track of workflows, job information, and resource utilization and provenance in an SQL database.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a web dashboard.
* Alter and resume failed workflows.

Multi-platform Support
+++++++++++++++++++++++

* Support for DRMS such as SGE, LSF and DRMAA.  Adding support for more DRMs is very straightforward.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the SQLALchemy ORM.
* Well suited for cloud computing 
* Ability to run workflows on your local computing, which is often great for testing.

Bug Reports
____________

Please use the `Github Issue Tracker <https://github.com/LPM-HMS/Cosmos2/issues>`_.

Changelog
__________

2.0.1
++++++
Some pretty big changes here, incurred during a hackathon at Invitae where a lot of feedback and contributions were received.  Primarily, the api was simplified and made
more intuitive.  A new Cosmos primitive was created called a Dependency, which we have found extremely useful for generalizing subworkflow recipes.
This API is now considered to be much more stable.

* Renamed Execution -> Workflow
* Reworked Workflow.add_task() api, see its docstring.
* Renamed task.tags -> task.params.
* Require that a task's params do not have keywords that do not exist in a task's functions parameters.
* Require that a user specify a task uid (unique identifer), which is now used for resuming instead of a Task's params.
* Created Cosmos.api.Dependency, which provides a way to specify a parent and input at the same time.
* Removed one2one, one2many, etc. helpers.  Found this just confused people more than helped.
* Various stability improvements to the drmaa jobmanager module
