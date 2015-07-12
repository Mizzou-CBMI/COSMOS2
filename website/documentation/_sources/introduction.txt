.. _introduction:

Introduction
============

Cosmos is a workflow management system for Python.  It allows you to efficiently program complex workflows of command line tools that automatically take
advantage of a compute cluster, and provides a web interface to monitor, debug, and analyze your jobs.

Features
________

* Written in python which is easy to learn, powerful, and popular.  A programmer with limited experience can begin writing Cosmos workflows right away.
* Powerful syntax and system for the creation of complex workflows.
* Keeps track of workflows, job information, and resource utilization and provenance in an SQL database.
* The ability to visualize all jobs and job dependencies as a convenient image.
* Monitor and debug running workflows, and a history of all workflows via a webinterface.

Multi-platform Support
______________________

* Support for :term:`DRMS` such as SGE, LSF, PBS/Torque, and Condor by utilizing :term:`DRMAA` 
* Supports for MySQL, PosgreSQL, Oracle, SQLite by using the :term:`Django` ORM
* Extremely well suited for cloud computing