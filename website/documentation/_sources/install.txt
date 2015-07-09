.. _install:

Install
=======

Requirements
_______________________________________

* The only other requirement is that :term:`DRMAA` is installed on the system if you want Cosmos to submit
  jobs to a :term:`DRMS` like LSF or Grid Engine.

* Cosmos requires python2.6 or python2.7 and is completely untested on python3.

* For :ref:`local`, it is highly recommended that you install `Ubuntu <http://www.ubuntu.com/>`_
  inside `VirtualBox <https://www.virtualbox.org/>`_.

* Many python libraries won't be able to install unless their dependent software is already
  installed on the system.  For example, pygraphviz requires graphviz-dev and
  python-mysql require python-dev libmysqlclient-dev.  If pip install is failing, try running:

.. code-block:: bash

    sudo apt-get update -y
    sudo apt-get install python-dev libmysqlclient-dev graphviz graphviz-dev


Install Method
_______________

Install Cosmos in a virtual environment using
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_.
This will make sure all python libraries and files related to Cosmos are installed to a sandboxed location in
:file:`$HOME/.virtualenvs/cosmos`.

.. code-block:: bash

    pip install virtualenvwrapper --user
    source $HOME/.local/bin/virtualenvwrapper.sh
    echo "\nsource $HOME/.local/bin/virtualenvwrapper.sh" >> ~/.bash_aliases
    echo "PATH=$HOME/.local/bin:$PATH" >> ~/.bash_aliases

    mkvirtualenv cosmos --no-site-packages
    cd /dir/to/install/Cosmos/to
    pip install distribute --upgrade
    git clone git@github.com:ComputationalBiomedicine/Cosmos.git --depth=1
    cd Cosmos
    pip install .


Cosmos will be installed to its own python virtual environment, which you can activate by executing the following
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_ command:

.. code-block:: bash

    $ workon cosmos

Make sure you execute :command:`workon cosmos` anytime you want to interact with Cosmos, or run a script
that uses Cosmos.  Deactivate the virtual environment by executing:

.. code-block:: bash

    $ deactivate


Experimental Features
_________________________

Optionally, if you want the experimental graphing capabilities to automatically summarize
computational resource usage, R and the R package ggplot2 are required.

.. code-block:: bash

   sudo apt-get install r graphviz-dev # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")

