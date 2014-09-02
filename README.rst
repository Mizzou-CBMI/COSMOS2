Building Docs
=============

.. code-block:: bash

    git clone git@github.com:LPM-HMS/Cosmos3.git
    cd Cosmos3
    virtualenv ve
    source ve/bin/activate
    pip install develop
    cd docs
    pip install sphinx sphinx_rtd_theme
    make html
    open build/html/index.html

