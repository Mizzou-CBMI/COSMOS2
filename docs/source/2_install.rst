Installation
=============


To install, clone the repository.  This procedure assumes you're using `VirtualEnv <http://virtualenv.readthedocs.org/en/latest/>`_:

.. code-block:: bash

    cd /dir/to/install/Cosmos/to
    mkvirtualenv ve
    source ve/bin/activate
    pip install cosmos-wfm

    # Optional, but highly recommended:
    sudo apt-get graphviz graphviz-dev  # or brew install graphviz for mac
    pip install pygraphviz # requires graphviz