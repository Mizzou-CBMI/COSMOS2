.. _tools:

.. py:module:: cosmos.Tool

Tools
===============

A :class:`~cosmos.Tool` represents an executable (like echo, cat, or paste, or script) that is run from the command line.
A tool is a class that overrides :py:class:`~Tool`, and implements the method :py:meth:`~Tool.cmd`.  Each instance of
:class:`~cosmos.Tool` corresponds to one :class:`Task` of the :term:`DAG`.

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
        drm = 'local' # run as a subprocess on the local machine, rather than submitting to the DRM
        skip_profile = True # skip profiling this job
        must_succeed = False # run the children of this job even if it fails


        def cmd(self, lines=False, in_txt=aif(format='txt'), out_txt=aof('count.txt')):
            l = ' -l' if lines else ''
            return r"""
                wc{l} {in_txt} > {out_txt}
                """.format(**locals())

Abstract Input File
--------------------
An :func:`~cosmos.abstract_input_taskfile` specifies the input file(s) required by a Tool.  All parents will be searched for output_files
that match both the name and/or format specified (at least the name or format must be specified).  The name and format
are regular expressions.  For example if you're expecting two .txt files but only know one name, you could do something
like this:

.. code-block:: python

    text1 = aif(name='(?!expected_name)',format='txt') # this aif's name matches anything that is NOT "expected_name"
    text2 = aif(name='expected_name', format='txt')

You can also override the signature of Tools for extremely special cases:

.. code-block:: python

    class ToolB(Tool):
        def cmd(self, aif=(format='txt|txt.gz')):
            pass

    class ToolB(ToolA):
        def cmd(self, aif=('very_specific_name','txt|txt.gz')):
            pass

Cardinality
_______________
The cardinality (The `n` parameter), is enforced such that ``n`` number of input_files should match.  By default,
the cardinality of each abstract_input_file is ``==1``, but this can be changed using the ``n`` parameter: ``itf(format='txt', n='>=1')``).
If you specify a cardinality where there may be more than 1, for example ``n='>=1'``, the ``cmd()`` parameter will be passed a list
of input files than output files.

Abstract Output File
--------------------
An :func:`~cosmos.abstract_output_taskfile` specifies an output file that a tool will generate.
Cardinality of output_files cannot be specified and is always 1.


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
.. autofunction:: cosmos.abstract_input_taskfile

.. autofunction:: cosmos.abstract_output_taskfile