.. _introduction:

Introduction
============

Cosmos is a workflow management system for Python.  It allows you to efficiently program complex workflows of command line tools that automatically take
advantage of a compute cluster, and provides a web dashboard to monitor, debug, and analyze your jobs.  Cosmos is
able to scale on a traditional cluster such as :term:`LSF` or :term:`SGE` with a shared filesystem.  It is especially
powerful when combined with spot instances on `Amazon Web Services <aws.amazon.com>`_ and
`StarCluster <http://star.mit.edu/cluster/docs/latest/>`_

Cosmos is owned by and copywrite Harvard Medical School.  The original manuscript was published in
Bioinformatics `Here <http://bioinformatics.oxfordjournals.org/content/early/2014/07/24/bioinformatics.btu385>`.

Features
________

* Written in python which is easy to learn, powerful, and popular.  A programmer with limited experience can begin writing Cosmos workflows right away.
* Powerful formal syntax and system for the creation of complex and highly parallelized workflows.
* Reusable recipes and definitions of tools allows for DRY code.
* Keeps track of workflows, job information, and resource utilization and provenance in an SQL database.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a dashboard.
* Alter and resume failed workflows

Multi-platform Support
______________________

* Support for :term:`DRMS` such as SGE, LSF.  term:`DRMAA` coming soon.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the :term:`Sqlalchemy` ORM
* Extremely well suited for cloud computing