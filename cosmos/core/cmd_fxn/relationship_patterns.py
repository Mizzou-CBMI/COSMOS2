import itertools as it
from ...models.Task import Task


def group(tasks, by):
    """
    A way to create common Many2one relationships, works similarly to a SQL GROUP BY

    :param iterable tasks: tasks to divide into groups
    :param list[str] by: the tag keys with which to create the groups.  Tasks with the same tag values of these keys
      will be partitioned into the same group, similar to a groupby.
    :yields dict, list[Tasks]: The common tags for this group, and the list of Tasks with those tags.
    """
    tasks = list(tasks)
    assert all(isinstance(t, Task) for t in tasks), '`tasks` must be an iterable of Tasks'

    def f(task):
        try:
            return {k: task.tags[k] for k in by}
        except KeyError as k:
            raise KeyError('keyword %s is not in the tags of %s' % (k, task))

    for group_tags, parent_group in it.groupby(sorted(tasks, key=f), f):
        yield group_tags.copy(), list(parent_group)


def one2one(execution, cmd_fxn, parents, tag=None, out_dir=None):
    """
    :param func cmd_fxn: the function that runs the command
    :param itrbl(Task) parents: A child tool will be created for each element in this list.
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str out_dir: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.
    :yields Tool: New tools.
    """
    if tag is None:
        tag = dict()

    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for parent in parents:
            new_tags = parent.tags.copy()
            new_tags.update(tag)
            yield execution.add_task(cmd_fxn, tags=new_tags, parents=[parent], out_dir=out_dir or parent.output_dir)

    return list(g())


def many2one(execution, cmd_fxn, parents, groupby, tag=None, out_dir=''):
    """
    :param func cmd_fxn: the function that runs the command
    :param list(str) groupby: A list of keys to groupby.  Parents will be grouped if they have the same values in
        their tags given by `groupby`.
    :param itrbl(Task) parents: An group of parents to groupby
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str|callable out_dir: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.  Alternatively use a callable who's parameter are tags and returns
        a str.  ie. ``out_dir=lambda tags: '{color}/' if tags['has_color'] else 'square/'``
    :yields: new Tools
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for new_tags, parent_group in group(parents, groupby):
            new_tags.update(tag)
            yield execution.add_task(cmd_fxn, tags=new_tags, parents=parent_group,
                                     out_dir=out_dir(new_tags) if hasattr(out_dir, '__call__') else out_dir)

    return list(g())


def combinations(splitby):
    for items in it.product(*[[(k, v) for v in l] for k, l in splitby.items()]):
        yield dict(items)


def one2many(execution, cmd_fxn, parents, splitby, tag=None, out_dir=''):
    """
    :param dict splitby: a dict who's values are lists, ex: dict(color=['red','blue'], shape=['square','circle'])
    :return:
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for parent in parents:
            new_tags = parent.tags.copy()
            for split_tags in combinations(splitby):
                new_tags.update(split_tags)
                new_tags.update(tag)
                yield execution.add_task(cmd_fxn, tags=new_tags, parents=[parent],
                                         out_dir=out_dir(new_tags) if hasattr(out_dir, '__call__') else out_dir)

    return list(g())
