Installation
=============

Make sure you have graphviz installed, if you want to use visualizations of your workflows.

.. code-block:: bash

    sudo apt-get graphviz graphviz-dev


To install, clone the repository.  This procedure assumes you're using `<VirtualEnv http://virtualenv.readthedocs.org/en/latest/`_:

.. code-block:: bash

    cd /dir/to/install/Cosmos/to
    mkvirtualenv ve
    source ve/bin/activate
    pip install pip distribute -U
    git clone git@github.com:LPM-HMS/Cosmos3.git --depth=1
    cd Cosmos
    pip install . # alternatively, use pip install devlop

