.. _tools:

.. py:module:: cosmos.models.Tool

Defining Tools
===============

A tool represents an executable (like echo, cat, or paste, or script) that is run from the command line.
A tool is a class that overrides :py:class:`~Tool`, and implements the method :py:meth:`~Tool.cmd`.

.. code-block:: python

    from cosmos import Tool, abstract_input_file as itf, abstract_output_file as otf

    class WordCount(Tool):
        """
        Count the number of words, and write it to another text file.
        """
        inputs = [itf(format='txt', n=1)]
        outputs = [otf('count','txt')]

        # You change change these defaults
        mem_req = 1*1024 # MB of RAM
        cpu_req = 1 # cores
        time_req = 60 # mins

        # Or specify these options
        drm = 'local' # run as a thread, rather than submitting to DRM
        skip_profile = True # skip profiling this job
        must_succeed = False # run the children of this job even if it fails


        def cmd(self, inputs, outputs):
            in_txt = inputs[0]
            out_txt = outputs[0]
            return r"""
                wc {in_txt} > {out_txt}
                """.format(**locals())

Abstract Input File
--------------------
An abstract_input_file specifies a type of input file(s) required by a Tool.  All parents will be searched for output_files
that match both the name and format specified (at least the name or format must be specified).
The order that you specify the abstract_input_files will be the order they arrive in the ``inputs`` parameter.

Cardinality
***********
The cardinality is enforced such that exactly ``n`` number of input_files should match.  By default, the cardinality of each
abstract_input_file is 1, but this can be changed using the ``n`` parameter (for example, *itf(format='txt', n='>=1')*).

Abstract Output File
--------------------
An abstract_output_file specifies an output file that a tool will generate.
The order that you specify the abstract_output_files will be the order they arrive in the ``outputs`` parameter.


API
-----------

Tool
*****
.. automodule:: cosmos.models.Tool
    :members: Tool, Input

.. automodule:: cosmos.models.TaskFile
    :members: abstract_input_file, abstract_output_file