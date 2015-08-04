.. _glossary:

Glossary
========

.. glossary::

    LSF
      Platform LSF is a commercial :term:`DRMS`

    SGE
        Sun Grid Engine is a commercial :term:`DRMS`

    GE
        Grid Engine is an open source version of :term:`SGE`

    DRMS
        Distributed Resource Management System.  This is the underlying queuing
        software that manages jobs on a cluster.
        Examples include :term:`LSF`, and :term:`SGE`

    DRMAA
        Distributed Resorce Management Application API.  A standard library that
        is an abstraction built on top of :term:`DRMS`
        so that the same application code can seamlessly run on any :term:`DRMS`
        that supports DRMAA

    DAG
        Directed Acyclic Graph.  A DAG is used byCosmos to describe a workflow's jobs and their dependencies on each other.

    Django
        A Python web framework that much of Cosmos is built on.