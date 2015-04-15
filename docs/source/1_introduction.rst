.. _introduction:

Introduction
============

Cosmos is a workflow management system for Python.  It allows you to efficiently program complex workflows of command line tools that automatically take
advantage of a compute cluster, and provides a web dashboard to monitor, debug, and analyze your jobs.  Cosmos is
able to scale on a traditional cluster such as :term:`LSF` or :term:`SGE` with a shared filesystem.  It is especially
powerful when combined with spot instances on `Amazon Web Services <aws.amazon.com>`_ and
`StarCluster <http://star.mit.edu/cluster/docs/latest/>`_.


History
___________

Cosmos was originally created at Harvard Medical School and the `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/07/24/bioinformatics.btu385>`_ was published in
`Bioinformatics <http://bioinformatics.oxfordjournals.org/>`_.
Since then, it has been completely re-written and open-sourced by the originiating author, in a collaboration between
`The Lab for Personalized Medicine <http://lpm.hms.harvard.edu/>`_, a bioinformatics lab, and
`Invitae <http://invitae.com`_, a genetic sequencing diagnostics lab.

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

* Support for :term:`DRMS` such as SGE, LSF.  :term:`DRMAA` coming soon.  Adding support for DRMs is very straightforward.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the :term:`Sqlalchemy` ORM
* Extremely well suited for cloud computing, especially when used in conjuection with `AWS <http://aws.amazon.com>`_ and `StarCluster <http://star.mit.edu/cluster/>`_.