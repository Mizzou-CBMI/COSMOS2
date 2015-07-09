COSMOS: A Python library for massively parallel workflows
=========================================================

**COSMOS** is a Python library for workflow management that allows formal description of pipelines and partitioning of jobs, focused on next-generation sequencing analysis. In addition, it includes a user-interface for tracking the progress of jobs, abstraction of the queuing system and fine-grained control over the workflow. Workflows can be created on traditional computing clusters as well as cloud-based services.  It is developed jointly by the `Laboratory for Personalized Medicine <http://lpm.hms.harvard.edu>`_ at Harvard Medical School and the `Wall Lab <http://wall-lab.stanford.edu/>`_ at Stanford University.  

Citing COSMOS
-------------
Gafni, Luquette, Lancaster, Hawkins, Jung, Souilmi, Wall, Tonellato "COSMOS: Python library for massively parallel workflows" *Bioinformatics* 2014.  in press.


Downloading COSMOS
-------------------
COSMOS is available for `download <http://cosmos.hms.harvard.edu/download/>`_ for academic and research purposes.

.. warning::

    COSMOS is copyright by Harvard Medical School 2012-2014
    
.. comment::

    * :ref:`introduction` -- What is Cosmos?
    * :ref:`install` -- Installation instructions.
    * :ref:`config` -- Configuration instructions.
    * :ref:`tutorial` -- Workflow examples step by step.
    * :ref:`cli` -- Manage your workflows and database from the command line.
    * :ref:`shell` -- Explore your workflows interactively in an IPython shell.
    * :ref:`API` -- Module API.

.. toctree::
    :numbered:
    :maxdepth: 4

    introduction
    install
    config
    getting_started
    ezflow/index
    cli
    shell
    advanced
    faq
    API/index
    glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`glossary`
* :ref:`modindex`
* :ref:`search`
