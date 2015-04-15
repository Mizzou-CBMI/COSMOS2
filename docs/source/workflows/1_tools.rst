.. _tools:

.. py:module:: cosmos.models.Tool

Tools
===============

A :class:`Tool` represents an executable (like echo, cat, or paste, or script) that is run from the command line.
A tool is a class that overrides :py:class:`~Tool`, and implements the method :py:meth:`~Tool.cmd`.  Each instance of
:class:`Tool` corresponds to one :class:`Task` of the :term:`DAG`.

.. code-block:: python

    from cosmos import Tool, abstract_input_file as aif, abstract_output_file as aof

    class WordCount(Tool):
        """
        Count the number of words, and write it to another text file.
        """
        # You change change these defaults
        mem_req = 1*1024 # MB of RAM
        cpu_req = 1 # cores
        time_req = 60 # mins

        # Or specify these options
        drm = 'local' # run as a thread, rather than submitting to DRM
        skip_profile = True # skip profiling this job
        must_succeed = False # run the children of this job even if it fails


        def cmd(self, lines=False, in_txt=aif(format='txt'), out_txt=aof('count.txt')):
            l = ' -l' if lines else ''
            return r"""
                wc{l} {in_txt} > {out_txt}
                """.format(**locals())

Abstract Input File
--------------------
An :meth:`abstract_input_file` specifies a type of input file(s) required by a Tool.  All parents will be searched for output_files
that match both the name and/or format specified (at least the name or format must be specified).

Cardinality
***********
The cardinality (The :param:`n` parameter, is enforced such that ``n`` number of input_files should match.  By default,
    the cardinality of each
abstract_input_file is 1, but this can be changed using the ``n`` parameter (for example, ``itf(format='txt', n='>=1')``).
If you specify a cardinality where there may be more than 1, for example ``n='>=1'``, the parameter will be passed a list
of input files regardless how many were matched.

Abstract Output File
--------------------
An abstract_output_file specifies an output file that a tool will generate.
The order that you specify the abstract_output_files will be the order they arrive in the ``outputs`` parameter.
Cardinality of output_files cannot be specified and is always 1.


Tags
----

Every instance of a Tool (and it's relevant Task) has a dictionary of tags.  These tags are used for the following:

* A unique identifier.  No tool/task can have the same set of tags within the *same stage*.
* Parameters.  If a keyword in a tool's tags matches a parameter in it's ``cmd()``, it will be passed into the call to ``cmd()``.
  For example when ``cmd()`` is called by the execution engine of the tool instantiated by ``WordCount(tags=dict(lines=True, other='val'))``, it will be called like so:
  ``cmd(lines=True, ...)``.
* A way to group similar tasks together when defining the :term:`DAG`
* A way to look up particular tasks in the Web Interface or using the API.

API
-----------

Tool
*****
.. automodule:: cosmos.models.Tool
    :members: Tool, Input

.. automodule:: cosmos.models.TaskFile
    :members: abstract_input_file, abstract_output_file