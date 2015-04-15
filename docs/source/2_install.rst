Installation
=============

If you want to use visualizations of your workflows, make sure you have graphviz and pygraphviz installed (recommended).

.. code-block:: bash

    sudo apt-get graphviz graphviz-dev


To install, clone the repository.  This procedure assumes you're using `VirtualEnv <http://virtualenv.readthedocs.org/en/latest/>`_:

.. code-block:: bash

    cd /dir/to/install/Cosmos/to
    mkvirtualenv ve
    source ve/bin/activate
    pip install pip distribute -U
    git clone git@github.com:LPM-HMS/Cosmos3.git Cosmos --depth=1
    cd Cosmos
    pip install .

    # Optional, but recommended:
    apt-get install graphviz  # or brew install graphviz for mac
    pip install pygraphviz

We also recommend you use `virtualenvwrapper <https://virtualenvwrapper.readthedocs.org/en/latest/>`_.

.. code-block::

    mkvirtualenv myproject
    git clone git@github.com:LPM-HMS/Cosmos3.git Cosmos --depth=1
    cd Cosmos
    pip install .

    # Optional, but recommended:
    apt-get install graphviz  # or brew install graphviz for mac
    pip install pygraphviz