**COSMOS** is a Python library for workflow management that allows formal description of pipelines and partitioning of jobs. In addition, it includes a user-interface for tracking the progress of jobs, abstraction of the queuing system and fine-grained control over the workflow. Workflows can be created on traditional computing clusters as well as cloud-based services.  It is developed jointly by the `Laboratory for Personalized Medicine <http://lpm.hms.harvard.edu>`_ at Harvard Medical School and the `Wall Lab <http://wall-lab.stanford.edu/>`_ at Stanford University.  It is available for academic and research purpose under the terms described in `LICENSE.md <https://github.com/LPM-HMS/Cosmos2/blob/master/LICENSE.md>`_.

For more information and the full documentation please visit: http://cosmos.hms.harvard.edu

-------
Building Docs
-------
 
.. code-block:: bash
 
    git clone git@github.com:LPM-HMS/Cosmos3.git
	cd Cosmos3
	virtualenv ve
	source ve/bin/activate
	pip install .
	cd docs
	pip install sphinx sphinx_rtd_theme
	make html
	open build/html/index.html
 
