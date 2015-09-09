"""
THIS ENTIRE MODULE IS DEPRECATED
"""

import itertools as it

from .. import Tool, Task


def make_dict(*args, **kwargs):
    """
    :param args: a list of dicts, or Tasks (for Tasks, their tags will be used)
    :param kwargs: a list of extra key/vals to add to the dict
    :return: a merge of all the dicts in args and kwargs
    """
    r = dict()
    for elem in args:
        if isinstance(elem, Task):
            elem = elem.tags
        elif not isinstance(elem, dict):
            raise '%s is not a dict' % elem
        r.update(elem)
    r.update(kwargs)
    return r


def one2one(tool_class, tasks, tag=None, out=None):
    """
    :param cosmos.Tool tool_class: a subclass of Tool to create new tasks with.
    :param itrbl(Task) tasks: A child tool will be created for each element in this list.
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str out: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.
    :yields Tool: New tools.
    """
    if tag is None:
        tag = dict()

    assert isinstance(tag, dict), '`tag` must be a dict'
    for parent in tasks:
        new_tags = parent.tags.copy()
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent, out=out or parent.output_dir)


def _group_paths(list_of_files_tag_tuples, by):
    """
    Same as group, but takes as input [(file1, dict), (file2, dict) instead]
    Useful for grouping together input files

    :param list[(str, dict)] list_of_files_tag_tuples:
    :param list[str] by: see :func:`group`

    :yields: dict, list[str]: The common tags for this group, a list of file_paths
    """

    def f((file_path, tags)):
        try:
            return {k: tags[k] for k in by}
        except KeyError as k:
            raise KeyError('keyword %s is not in the tags of %s' % (k, (file_path, tags)))


    for group_tags, tuple_group in it.groupby(sorted(list_of_files_tag_tuples, key=f), f):
        yield group_tags.copy(), list(tuple_group)


def group(tasks_or_tuples, by):
    """
    A way to create common Many2one relationships, works similarly to a SQL GROUP BY

    :param iterable tasks_or_tuples: tasks_or_tuples to divide into groups
    :param list[str] by: the tag keys with which to create the groups.  Tasks with the same tag values of these keys
      will be partitioned into the same group, similar to a groupby.
    :yields dict, list[Tasks]: The common tags for this group, and the list of Tasks with those tags.
    """
    tasks_or_tuples = list(tasks_or_tuples)
    if isinstance(tasks_or_tuples[0], tuple):
        assert isinstance(tasks_or_tuples[0][0], str) and isinstance(tasks_or_tuples[0][1],
                                                                     dict), 'Tuple must be of type (str, dict)'
        for x in _group_paths(tasks_or_tuples, by):
            yield x
    elif not isinstance(tasks_or_tuples[0], Task):
        raise AssertionError('`tasks_or_tuples` must be an iterable of Tasks or tuples')


    def f(task):
        try:
            return {k: task.tags[k] for k in by}
        except KeyError as k:
            raise KeyError('keyword %s is not in the tags of %s' % (k, task))


    for group_tags, parent_group in it.groupby(sorted(tasks_or_tuples, key=f), f):
        yield group_tags.copy(), list(parent_group)


reduce_ = group  # deprecated name


def many2one(tool_class, parents, groupby, tag=None, out=''):
    """
    :param cosmos.Tool tool_class: a subclass of Tool to create new tasks with.
    :param list(str) groupby: A list of keys to groupby.  Parents will be grouped if they have the same values in
        their tags given by `groupby`.
    :param itrbl(Task) parents: An group of parents to groupby
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str|callable out: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.  Alternatively use a callable who's parameter are tags and returns
        a str.  ie. ``out_dir=lambda tags: '{color}/' if tags['has_color'] else 'square/'``
    :yields: new Tools
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    for new_tags, parent_group in group(parents, groupby):
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent_group, out=out(new_tags) if hasattr(out, '__call__') else out)


def many2many(tool_class, parents, groupby, splitby, tag=None, out=''):
    """
    :param dict splitby: a dict who's values are lists, ex: dict(color=['red','blue'], shape=['square','circle'])
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    for group_tags, parent_group in group(parents, groupby):
        parent_group = list(parent_group)
        if hasattr(splitby, '__call__'):
            g = splitby(group_tags)
        else:
            g = combinations(splitby)

        for new_tags in g:
            new_tags.update(group_tags)
            new_tags.update(tag)
            yield tool_class(tags=new_tags, parents=parent_group,
                             out=out(group_tags) if hasattr(out, '__call__') else out)


def combinations(splitby):
    for items in it.product(*[[(k, v) for v in l] for k, l in splitby.items()]):
        yield dict(items)


def one2many(tool_class, parents, splitby, tag=None, out=''):
    """
    :param dict splitby: a dict who's values are lists, ex: dict(color=['red','blue'], shape=['square','circle'])
    :return:
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    for parent in parents:
        new_tags = parent.tags.copy()
        for split_tags in combinations(splitby):
            new_tags.update(split_tags)
            new_tags.update(tag)
            yield tool_class(tags=new_tags, parents=[parent], out=out(new_tags) if hasattr(out, '__call__') else out)
