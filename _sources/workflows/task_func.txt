.. _tools:

.. py:module:: cosmos.core

Task Functions
==================

A `Task Function` (or `task_func`) represents a command-line tool (like echo, cat, paste, or a custom script).  It returns a String,
which gets written to the filesystem as a shell-script and submitted as a job.  It is just a plain old python function.

.. code-block:: python

    from cosmos.api import find, out_dir

    def word_count(use_lines=False, in_txt=find('txt$'), out_txt=out_dir('count.txt')):
        l = ' -l' if use_lines else ''
        return r"""
            wc{l} {in_txt} > {out_txt}
            """.format(**locals())

    word_count(True, '/path/to/input_file.txt', 'output_count.txt')
    >>> "wc -l /path/to/input_file.txt output_count.txt"

There are some special things about `task_func` parameters that Cosmos will recognize.

* A function parameter that starts with `in_` is an input_file.
* A function parameter that starts with `out_` is an output_file.
