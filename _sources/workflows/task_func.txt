.. _task_func:

.. py:module:: cosmos.core

Task Functions
==================

A `Task Function` (or `task_func`) represents a command-line tool (like echo, cat, paste, or a custom script).  It returns a string,
which gets written to the filesystem as a shell-script and submitted as a job.  It is just a plain old python function.

.. code-block:: python

    def word_count(in_txt, out_txt, use_lines=False):
        l = ' -l' if use_lines else ''
        return r"""
            wc{l} {in_txt} > {out_txt}
            """.format(**locals())

    >>> word_count(True, '/path/to/input_file.txt', 'output_count.txt')
    "wc -l /path/to/input_file.txt output_count.txt"

There are some special things about `task_func` parameters that Cosmos will recognize.

* A function parameter that starts with `in_` is an input_file.
* A function parameter that starts with `out_` is an output_file.
* The `cpu_req` parameter will be used to set any core requests to a :term:`DRM`.  Setting defaults
  will work as expected.
