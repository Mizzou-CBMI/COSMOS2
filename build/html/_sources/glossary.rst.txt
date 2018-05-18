.. _glossary:

Glossary
========

.. glossary::

    LSF
      Platform LSF is a commercial :term:`DRM`

    SGE
        Sun Grid Engine is a commercial :term:`DRM`

    GE
        Grid Engine is an open source version of :term:`SGE`

    DRM
        Distributed Resource Management System.  This is the underlying queuing
        software that manages jobs on a cluster.
        Examples include :term:`LSF`, and :term:`SGE`

    DRMAA
        Distributed Resorce Management Application API.  A standard library that
        is an abstraction built on top of :term:`DRM`
        so that the same application code can seamlessly run on any :term:`DRM`
        that supports DRMAA

    DAG
        Directed Acyclic Graph.  A DAG is used byCosmos to describe a workflow's jobs and their dependencies on each other.

    Flask
        A Python web framework Cosmos uses for its web-interface

    Sqlalchemy
        A popular Python ORM that Cosmos uses