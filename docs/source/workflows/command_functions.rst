.. _tools:

.. py:module:: cosmos.core.cmd_fxn

Command Functions
==================

A `command_fxn` (or `cmd_fxn`) represents a command-line tool (like echo, cat, paste, or a custom script).  It returns a String,
which gets written to the filesystem as a shell-script and submitted as a job.  It is a plain old python function.

.. code-block:: python

    from cosmos.api import find, out_dir

    def word_count(use_lines=False, in_txt=find('txt$'), out_txt=out_dir('count.txt')):
        l = ' -l' if use_lines else ''
        return r"""
            wc{l} {in_txt} > {out_txt}
            """.format(**locals())

    word_count(True, '/path/to/input_file.txt', 'output_count.txt')

    >>> wc -l /path/to/input_file.txt output_count.txt

There are some special things about a `command function` that Cosmos will recognize.

* A function parameter that starts with `in_` is an input_file.
* A function parameter that starts with `out_` is an output_file.
* The :func:`find` default can be specified for input files (and almost always is).  This will cause Cosmos, by default,
  to search all of the parents of a Task for files that match the find's :param:`regex` parameter.
* :func:`out_dir` will automatically append the Task's output directory to the filename.
* :func:`forward` will automatically set an output_file to the input_file specified.

More on Find()
--------------

Cardinality
_______________
The cardinality (The `n` parameter), is enforced such that ``n`` number of input_files should match.  By default,
the cardinality of each abstract_input_file is ``==1``, but this can be changed using the ``n`` parameter: ``find(format='txt', n='>=1')``).
If you specify a cardinality where there may be more than 1, for example ``n='>=1'``, value passed into this input
file will be a list of file paths, rather than just a single file path.


Tags
----

Every instance of a Tool (and it's relevant Task) has a dictionary of tags.  These tags are used for the following:

* A unique identifier.  No tool/task can have the same set of tags within the *same stage*.
* Parameters.  If a keyword in a tool's tags matches a parameter in it's ``cmd()`` method, it will be passed into the call to ``cmd()`` as a parameter.
  For example when ``cmd()`` is called by Cossmos for the tool ``WordCount(tags=dict(lines=True, other='val'))``, it will be called like this:
  ``cmd(lines=True, other='val',...)``.
* A way to group similar tasks together when defining the :term:`DAG`.
* A way to look up particular tasks in the Web Interface or using the API.

API
-----------

Tool
_______


.. automodule:: cosmos.models.Tool
    :members: Tool

Abstract I/O Files
+++++++++++++++++++++
.. autofunction:: cosmos.api.find

.. autofunction:: cosmos.api.out_dir

.. autofunction:: cosmos.api.forward