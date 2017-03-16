.. _introduction:

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