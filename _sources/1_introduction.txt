.. _introduction:

Introduction
============
`COSMOS <http://cosmos.hms.harvard.edu>`_ is a Python library for workflow management and distributed computing.
 It includes a user-interface for tracking the progress of jobs, abstraction of the queuing system and fine-grained control over the workflow.
 Workflows can be created on traditional computing clusters as well as cloud-based services, or run on local machines.
 It is developed jointly by the `Laboratory for Personalized Medicine <http://lpm.hms.harvard.edu/>`_ at Harvard Medical School,
 the `Wall Lab <wall-lab.stanford.edu>`_ at Stanford University, and
`Invitae <http://invitae.com>`_, a clinical genetic sequencing diagnostics laboratory.

COSMOS allows you to efficiently program complex workflows of command line tools that automatically take
advantage of a compute cluster, and provides a web dashboard to monitor, debug, and analyze your jobs.  Cosmos is
able to scale on a traditional cluster such as :term:`LSF` or :term:`SGE` with a shared filesystem.  It is especially
powerful when combined with spot instances on `Amazon Web Services <aws.amazon.com>`_ and
`StarCluster <http://star.mit.edu/cluster/>`_.

Cite COSMOS
___________

Gafni E, Luquette LJ, Lancaster AK, Hawkins JB, Jung J-Y, Souilmi Y, Wall DP, Tonellato PJ: COSMOS: Python library for massively parallel workflows. Bioinformatics 2014. doi: `10.1093/bioinformatics/btu385 <http://bioinformatics.oxfordjournals.org/content/30/20/2956>`_.

History
___________

Since the original publication, COSMOS has been re-written and open-sourced by the original author.  It was primarily designed by the author to create scientific
data pipelines for Next Generation Sequencing, and he continues to use it for this today.  However, COSMOS is a general distributed computing workflow library, not tied to
bioinformatics, and used in other fields such as image processing.

Features
_________

* Simple syntax for the creation of complex and highly parallelized workflows.
* Reusable recipes and definitions of tools and sub-workflows allows for DRY code.
* Keeps track of workflows, job information, and resource utilization and provenance in an SQL database.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a web dashboard.
* Alter and resume failed workflows.

Workflow Structure
++++++++++++++++++++
There are 4 objects that get used in Cosmos.  The first is the `:class:~cosmos.api.Cosmos` class, which represents a Cosmos session.  It is initialized
with a SQL database, which is used for recording all the objects in a workflow.  Among other things, this allows workflows to be resumed and monitored.

The object hierarchy of a workflow is: *Workflow -> Stage -> Task*.  Workflows have a one2many relationship to Stages, and Stages have a
one2many relationship to Tasks.

Tasks contain parent/child relationships to other Tasks (directed edges) which define dependencies, and this makes up the DAG.

The Zen of Cosmos
++++++++++++++++++

(`The Zen of Python <https://www.python.org/dev/peps/pep-0020/>`_)

* `Workflow Syntaxes` that try to create an abstraction or DSL to describe a :term:`DAG` are **bad**.  They work 90% of the time, and the 10% of the time they
 don't, you get into a lot of trouble.  Describing a DAG in COSMOS is very similar to constructing a DAG in any graph library.

* The definition of a DAG and the definition of a Task should be completely divorced from each other so that
  the same Task can be reused in different workflows.

* Tasks and recipes for the creation of dags should be re-usable and composable.

* It should be really easy to debug errors.


Multi-platform Support
+++++++++++++++++++++++

* Support for :term:`DRMS` such as SGE, LSF.  :term:`DRMAA` coming soon.  Adding support for more DRMs is very straightforward.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the :term:`Sqlalchemy` ORM.
* Extremely well suited for cloud computing, especially when used in conjuection with `AWS <http://aws.amazon.com>`_ and `StarCluster <http://star.mit.edu/cluster/>`_.
