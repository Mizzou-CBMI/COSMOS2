Conda
======

.. code-block:: bash

    rm -rf cosmos-wfm
    conda skeleton pypi cosmos-wfm
    # replace _ with - in more_itertools
    conda build cosmos-wfm/meta.yaml