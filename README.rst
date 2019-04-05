.. image:: https://travis-ci.org/Mizzou-CBMI/COSMOS2.svg?branch=master
    :target: https://travis-ci.org/Mizzou-CBMI/COSMOS2

The official cosmos website is hosted at `http://cosmos.hms.harvard.edu <http://cosmos.hms.harvard.edu>`_.

To chat with the author/other users (many of which use Cosmos to make bioinformatics NGS workflows), use gitter:

.. image:: https://badges.gitter.im/Join%20Chat.svg
    :alt: Join the chat at https://gitter.im/Mizzou-CBMI/COSMOS2
    :target: https://gitter.im/Mizzou-CBMI/Cosmos2?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Python3
=========
Cosmos now supports python3!


Documentation
==============

`http://mizzou-cbmi.github.io/COSMOS2/ <http://mizzou-cbmi.github.io/COSMOS2/>`_


Install
==========

.. code-block:: python

    pip install cosmos-wfm

    # Optional, recommended for visualizing Workflows:
    sudo apt-get graphviz graphviz-dev  # or brew install graphviz for mac
    pip install pygraphviz # requires graphviz

Introduction
============
Cosmos is a python library for creating scientific pipelines that run on a distributed computing cluster.  It is primarily designed and used for bioinformatics pipelines, but is general enough for any type of distributed computing workflow and is also used in fields such as image processing.  A Cosmos pipeline can run locally on a single machine or a traditional computing cluster like GridEngine, LSF, Condor, PBS/Torque, SLURM or any other Distributed Resource Manager (DRM) that supports `DRMAA <https://www.drmaa.org/>`__. Adding support for other DRMs is very straightforward, and support for `AWS Batch <https://aws.amazon.com/batch/>`__ is in the works. For those who want to use AWS, it pairs very well with AWS' new  `CfnCluster <https://aws.amazon.com/hpc/cfncluster/>`__.

Cosmos provides a simple api to specify complex job DAGs, a way to resume modified or failed workflows, uses SQL to store job information, and provides a web dashboard for monitoring and debugging.
It is different from libraries such as `Luigi <https://github.com/spotify/luigi>`__ or `Airflow <http://airbnb.io/projects/airflow/>`__ which are simultaneously trying to solve problems such as scheduling recurring tasks and listening for events.
Cosmos is very focused only on reproducible scientific pipelines, allowing it to have a very simple state.  There is a single process per Workflow which is a python script, and single process per Task which is a command inside a bash script.  When a Task fails, reproducing the exact
environment of a Task is as simple as re-running the bash script.  Cosmos is intended and useful for both one-off analyses and production software.  Users have reported analyzing >100 whole genomes (~50TB and tens of thousands of jobs) in
a single Workflow without issue.

History
___________
Cosmos was published as an Application Note in the journal `Bioinformatics <http://bioinformatics.oxfordjournals.org/>`_,
but has evolved a lot since it's original inception.  If you use Cosmos
for research, please cite it's `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/06/29/bioinformatics.btu385>`_. 

Since the original publication, it has been re-written and open-sourced by the original author, in a collaboration between
`The Lab for Personalized Medicine <http://lpm.hms.harvard.edu/>`_ at Harvard Medical School, the `Wall Lab <http://wall-lab.stanford.edu/>`_ at Stanford University, and
`Invitae <http://invitae.com>`_.  Invitae is a leading clinical genetic sequencing diagnostics laboratory where Cosmos is deployed in production and processes thousands of samples per month.  It is also used by various research groups around the world; if you use it for cool stuff please let us know!

Features
_________
* Written in python which is easy to learn, powerful, and popular.  A researcher or programmer with limited experience can begin writing Cosmos workflows right away.
* Powerful syntax for the creation of complex and highly parallelized workflows.
* Reusable recipes and definitions of tools and sub workflows allows for DRY code.
* Keeps track of workflows, job information, and resource utilization and provenance in an SQL database and log files.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a web dashboard.
* Alter and resume failed workflows.

Web Dashboard
_______________
.. figure:: docs/source/_static/imgs/web_interface.png
   :align: center
   
Multi-platform Support
+++++++++++++++++++++++
* Support for DRMS such as SGE, LSF, SLURM and others via DRMAA.  Adding support for more DRMs is very straightforward.
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the SQLALchemy ORM.
* Well suited for cloud computing 
* Ability to run workflows on your local computer, which is often great for testing.

Bug Reports
____________

Please use the `Github Issue Tracker <https://github.com/Mizzou-CBMI/Cosmos2/issues>`_.

Testing
__________
python setup.py test

.. code-block:: bash

    py.test

Building Docs
______________

In a python2.7 environment

.. code-block:: bash

    pip install ghp-import sphinx sphinx_rtd_theme
    cd docs
    make html
    ghp-import -n ./ -p


Cosmos Users
_________________

Please let us know if you're using Cosmos by sending a PR with your company or lab name and any relevant information.

* `GenomeKey <https://github.com/Mizzou-CBMI/GenomeKey>`__ - A GATK best practices variant calling pipeline.
* `PV-Key  <https://github.com/Mizzou-CBMI/PvKey>`__ - Somatic Tumor/normal variant calling pipeline.
* `MC-Key <https://bitbucket.org/shazly/mcgk>`__ - Multi-cloud implementation of GenomeKey.
* `Invitae <http://invitae.com>`__ - Clinical NGS sequencing laboratory.  Utilizes Cosmos for production variant calling pipelines and R&D analysis.
* `NGXBIO <https://ngxbio.com/>`__ - NGS Sequencing as a Service.
* `EnGenome <https://engenome.com/en/>`__ - Bioinformatics and NGS Analysis.
* `Freenome <https://freenome.com>`__ - Liquid Biopsy Sequencing Laboratory, specializing in Machine Learning

Publications using Cosmos
__________________________

1) Elshazly H, Souilmi Y, Tonellato PJ, Wall DP, Abouelhoda M (2017) MC-GenomeKey: a multicloud system for the detection and annotation of genomic variants. BMC Bioinformatics, 18(1), 49.

2) Souilmi Y, Lancaster AK, Jung JY, Rizzo E, Hawkins JB, Powles R, Amzazi S, Ghazal H, Tonellato PJ, Wall DP (2015) Scalable and cost-effective NGS genotyping in the cloud. BMC Medical Genomics, 8(1), 64.

3) Souilmi Y., Jung J-Y., Lancaster AK, Gafni E., Amzazi S., Ghazal H., Wall DP., Tonellato, P. (2015). COSMOS: cloud enabled NGS analysis. BMC Bioinformatics, 16(Suppl 2), A2. doi: 10.1186/1471-2105- 16-S2- A2

4) Gafni E, Luquette LJ, Lancaster AK, Hawkins JB, Jung J-Y, Souilmi Y, Wall DP, Tonellato PJ: COSMOS: Python library for massively parallel workflows. Bioinformatics (2014) 30 (20): 2956-2958. doi: 10.1093/bioinformatics/btu385

5) Hawkins JB, Souilmi Y, Powles R, Jung JY, Wall DP, Tonellato PJ (2013) COSMOS: NGS Analysis in the Cloud. AMIA TBI. BMC Medical Genomics


Changelog
__________

2.5.1
++++++

API Change!

* Removed Workflow.run(int: max_attempts) and replaced with Cosmos.start(int: default_max_attempts)
* Added Workflow.add_task(int: max_attempts) to specify individual Task retry numbers


2.5.0
++++++

* Added support for python3

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

