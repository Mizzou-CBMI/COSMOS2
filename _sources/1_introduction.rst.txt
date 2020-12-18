.. _introduction:

Introduction
============
Cosmos is a python library for creating scientific pipelines that run on a distributed computing cluster.
It is primarily designed and used for bioinformatics pipelines, but is general enough for any type of distributed computing workflow and is also used in fields such as image processing.

Cosmos provides a simple api to specify any job DAG using simple python code making it extremely flexible and inuitive
- you do *not* specify your DAG using json, CWL, groovy, or some other domain specific language.

Cosmos allows you to resume modified or failed workflows, uses SQL to store job information, and provides a web dashboard for monitoring and debugging.
It is different from libraries such as `Luigi <https://github.com/spotify/luigi>`__
or `Airflow <http://airbnb.io/projects/airflow/>`__ which also try to solve ETL problems such as scheduling recurring tasks and listening for events.

Cosmos is very focused on reproducible scientific pipelines, allowing it to have a very simple state.
There is a single process per Workflow which is a python script, and a single process per Task which is python function represented by an executable script.
When a Task fails, reproducing the exact environment of a Task is as simple as re-running the command script.  The same pipeline can
also easily be run on a variety of compute infrastructure: locally, in the cloud, or on a grid computing cluster.

Cosmos is intended and useful for both one-off analyses and production software.
Users have analyzed >100 whole genomes (~50TB and tens of thousands of jobs) in a single Workflow without issue, and some of the largest
clinical sequencing laboratories use it for the production and R&D workflows.


History
___________
Cosmos was published as an Application Note in the journal `Bioinformatics <http://bioinformatics.oxfordjournals.org/>`_,
but has evolved a lot since its original inception.  If you use Cosmos
for research, please cite its `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/06/29/bioinformatics.btu385>`_.

Since the original publication, it has been re-written and open-sourced by the original author, in a collaboration between
`The Lab for Personalized Medicine <http://lpm.hms.harvard.edu/>`_ at Harvard Medical School, the `Wall Lab <http://wall-lab.stanford.edu/>`_ at Stanford University, and
`Invitae <http://invitae.com>`_.  Invitae is a leading clinical genetic sequencing diagnostics laboratory where Cosmos is deployed in production and has processed hundreds of thousands of samples.
It is also used by various research groups around the world; if you use it for cool stuff please let us know!

Features
_________
* Written in python which is easy to learn, powerful, and popular.  A researcher or programmer with limited experience can begin writing Cosmos workflows right away.
* Powerful syntax for the creation of complex and highly parallelized workflows.
* Reusable recipes and definitions of tools and sub workflows allows for DRY code.
* Keeps track of workflows, job information, resource utilization and provenance in an SQL database and log files.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a web dashboard.
* Alter and resume failed workflows.


Multi-platform Support
+++++++++++++++++++++++
* Support for running pipelines locally
* Support for running pipelines on AWSBatch (new!)
* Support for running pipelines on DRMS such as SGE, LSF, SLURM and others via DRMAA.  Adding support for more DRMs is very straightforward.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the SQLALchemy ORM.