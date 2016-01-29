Overview
==========

Workflow Structure
++++++++++++++++++++
There are 4 objects that get used in Cosmos.  The first is the `:class:~cosmos.api.Cosmos` class, which represents a Cosmos session.  It is initialized
with a SQL database, which is used for recording all the objects in a workflow.  Among other things, this allows workflows to be resumed and monitored.

The object hierarchy of a workflow is: *Execution -> Stage -> Task*.  Executions have a one2many relationship to Stages, and Stages have a
one2many relationship to Tasks.

Tasks contain parent/child relationships to other Tasks (directed edges) which define dependencies, and this makes up the DAG.

The Zen of Cosmos
++++++++++++++++++

(`The Zen of Python <https://www.python.org/dev/peps/pep-0020/>`_)

* The definition of a DAG and the definition of a Task should be completely divorced from each other so that
  the same Task should be be reusable in different workflows.

* A Task is a function with inputs, outputs and parameters.

* The Task's inputs, outputs and parameters should contain sensible defaults, but be easily overridden.

* Recipes for the creation of dags should be re-usable and composable.

* It should be really easy to debug errors.

