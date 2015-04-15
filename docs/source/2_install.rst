Installation
=============

If you want to use visualizations of your workflows, make sure you have graphviz and pygraphviz installed (recommended).

.. code-block:: bash

    sudo apt-get graphviz graphviz-dev  # or brew install graphviz for mac


To install, clone the repository.  This procedure assumes you're using `VirtualEnv <http://virtualenv.readthedocs.org/en/latest/>`_:

.. code-block:: bash

    cd /dir/to/install/Cosmos/to
    mkvirtualenv ve
    source ve/bin/activate
    pip install cosmos-wfm

    # Optional, but highly recommended:
    pip install pygraphviz # requires graphviz


We recommend using `virtualenvwrapper <https://virtualenvwrapper.readthedocs.org/en/latest/>`_.

.. code-block:: bash

    mkvirtualenv myproject
    pip install cosmos-wfm
    pip install pygraphviz  # requires graphviz
