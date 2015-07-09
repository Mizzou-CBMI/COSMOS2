.. _tools:

Defining Tools
===============

A tool represents an executable (like echo, cat, or paste, or script) that is run from the command line.
A tool is a class that overrides :py:class:`~tool.Tool`, and defines :py:meth:`~tool.Tool.cmd`,
(unless the tool doesn't actually perform an operation, ie
:py:attr:`tool.Tool.NOOP` == True).

.. code-block:: python

    from cosmos.lib.ezflow.tool import Tool

    class WordCount(Tool):
        name = "Word Count"
        inputs = ['txt']
        outputs = ['txt']
        mem_req = 1*1024
        cpu_req = 1

        def cmd(self,i,s,p):
            return r"""
                wc {i[txt][0]} > $OUT.txt
                """

This tool will read a txt file, count the number of words, and write it to another text file.


See the :py:class:`Tool API <tool.Tool>` for more class attributes that can be overridden to obtain
various behaviors.


Defining Input Files
--------------------

An Input file is an instantiation of :py:class:`tool.INPUT`, which is just a Tool with
:py:attr:`tool.INPUT.NOOP` set to True, and a way to initialize it with a single output file from an existing
path on the filesystem.

An INPUT has one outputfile, which is an instance of :py:class:`Workflow.models.TaskFile`.  It has 3 important
attributes:

* ``name``: This is the name of the file, and is used as the key for obtaining it.  No Tool can
    have multiple TaskFiles with the same name.  Defaults to ``fmt``.
* ``fmt``: The format of the file.  Defaults to the extension of ``path``.
* ``path``: The path to the file. Required.

Here's an example of how to create an instance of an :py:class:`tool.INPUT` File:

.. code-block:: python

    from cosmos.lib.ezflow import INPUT

    input_file = INPUT('/path/to/file.txt',tags={'i':1})

``input_file`` will now be a tool instance with an output file called 'txt' that points to :file:`/path/to/file.txt`.

A more fine grained approach to defining input files:

.. code-block:: python

    from cosmos.lib.ezflow import INPUT
    INPUT(name='favorite_txt',path='/path/to/favorite_txt.txt.gz',fmt='txt.gz',tags={'color':'red'})


API
-----------

Tool
*****
.. automodule:: cosmos.lib.ezflow.tool
    :members:
