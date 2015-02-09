.. _recipes:

.. py:module:: cosmos.graph.rel

Recipes
========

Recipes are re-usable descriptions for how to construct and parallelize complicated workflows and ultimately create a :term:`DAG` of
Tasks.

.. code-block:: python

    from cosmos import Recipe

    r = Recipe()

    load = r.add_source([Input('/path/to/file.txt', tags=dict(key='word'))])

Relationships
-------------
There are 4 types of Relationships

One2one
+++++++
For each parent, one new tool will be generated.  Used by default in Recipe.add().  Child tasks will inherit
the same tags as their parent.

One2many
++++++++
For each parent, two or more new tools will be generated using all possible combinations provided.  For example:

.. code-block:: python

    from cosmos import rel

    stageA = recipe.add(tools.ToolA, parents=[load],
                 rel=rel.One2Many(split_by=dict(shape=['square','circle']), color=['red','blue'])]
             )

Would create 4 new tasks of type ``tools.MyTool`` with these tags added to them (parent tags are also inherited):

.. code-block:: python

    {'shape':'square','color':'red'}, {'shape':'square','color':'blue'},
    {'shape':'circle','color':'red'}, {'shape':'circle','color':'blue'}

Many2one
+++++++++
Parent tasks are grouped by common tags.  If a parent is missing one of the tags in reduce_by, it will be included in
*all* parent groups.  Children inherit all tags that the parent group have in common.

.. code-block:: python

    recipe.add(tools.MyTool2, parents=[stageA], rel=rel.Many2one(reduce_by=['color'])

Would generate two tasks.  One task would have two parents that had a color of red, and the other would have two parents
that had a color of blue.

Many2many
+++++++++
Essentially is a Many2one immediately followed by a One2many, and its parameters reflect exactly that.

.. code-block:: python

    stageB = recipe.add(tools.ToolB, parents=[stageB],
                     rel=rel.Many2many(reduce_by=['color'], split_by=dict(size=['small', 'large']))

Would group parents by size, then create two new tasks for each parent group with tags

API
-----------

.. automodule:: cosmos.graph.recipe
    :members: Recipe


.. automodule:: cosmos.models.Tool
    :members: Input


.. automodule:: cosmos.graph.rel
    :members: One2one, One2many, Many2one, Many2many

