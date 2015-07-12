.. _config:

Configuration
=============

1. Install Configuration File
_______________________________

Type `cosmos` at the command line, and generate a default configuration file in :file:`~/.cosmos/config.ini`.
Edit :file:`~/.cosmos/config.ini`, and configure it to your liking; the instructions are in the file.

.. _local:

Local Development and Testing
******************************

Setting DRM = local in your config file will cause jobs to be submitted as background
processes on the local machine using :py:mod:`subprocess`.Popen.  The `DRM = local` setting's
purpose is for testing and developing workflows, not computing on large datasets.

.. warning::

    Be careful how many resource intensive jobs your workflow submits at once when using `DRM = local`.
    Currently there's no way to set a ceiling on the number
    of processes being executed simultaneously, since this is supposed to be handled by a DRM.
    A feature to set a ceiling on concurrent processes may be added in the
    future.

.. hint::

    If you do not have linux installed and want to use this feature,
    consider installing `Ubuntu <http://www.ubuntu.com/>`_
    inside `VirtualBox <https://www.virtualbox.org/>`_.  Cosmos does not support Windows.


2. Create SQL Tables and Load Static Files
__________________________________________

Once you've configured Cosmos, setting up the SQL database tables is easy.  The web interface is a
:term:`Django` application, which requires you to run the collectstatic command.  This moves all the necessary image, css, and
javascript files to the ~/.cosmos/static/ directory.  Run these two commands after you've configured the database in the
cosmos configuration file.

.. code-block:: bash

   $ cosmos syncdb
   $ cosmos collectstatic

If you ever switch to a different database in your :file:`~/.cosmos/config.ini`, be sure to run `cosmos syncdb`
to recreate your tables.