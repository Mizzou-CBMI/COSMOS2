.. _examples:

Examples
======================

The easiest way to learn is often by example.  When you're learning to write your own workflows,
make liberal use of the :py:meth:`~cosmos.flow.dag.DAG.create_dag_img` and the `visualize` button
in the runweb interface.  More examples are available in the examples/ directory of the github repository.

It might look verbose, but that's because the Cosmos api is very *explicit*.  There's no DSL to string together your pipeline
like other workflow managers - we've found that works 80% of the time; the other 20% of the time it is a huge headache.
It's also worth pointing out that almost the entire API is showcased in these examples.  Despite the simplicity of the
API, you have all the flexibility in the world to define any DAG you like.


.. toctree::
    :numbered:
    :maxdepth: 2

    ex1_hello_world
    ex2_complete
    ex3_pycall
    awsbatch
    gpu
    env_variables