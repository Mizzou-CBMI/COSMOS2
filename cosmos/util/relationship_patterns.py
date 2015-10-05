import itertools as it
from ..models.Task import Task


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


def one2one(cmd_fxn, parents, tag=None, out_dir=None, stage_name=None):
    """
    :param func cmd_fxn: the function that runs the command
    :param itrbl(Task) parents: A child task will be created for each element in this list.
    :param dict tag: Tags to add to the Task's dictionary.  The Task will also inherit the tags of its parent.
    :param str out_dir: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.
    :yields Task: Tasks.
    """
    execution = parents[0].execution

    if tag is None:
        tag = dict()

    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for parent in parents:
            new_tags = parent.tags.copy()
            new_tags.update(tag)
            yield execution.add_task(cmd_fxn, tags=new_tags, parents=[parent], out_dir=out_dir or parent.output_dir, stage_name=stage_name)

    return list(g())


def many2one(cmd_fxn, parents, groupby, tag=None, out_dir='', stage_name=None):
    """
    :param func cmd_fxn: the function that runs the command
    :param list(str) groupby: A list of keys to groupby.  Parents will be grouped if they have the same values in
        their tags given by `groupby`.
    :param itrbl(Task) parents: An group of parents to groupby
    :param dict tag: Tags to add to the Task's dictionary.  The Task will also inherit the tags of its parent.
    :param str|callable out_dir: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.  Alternatively use a callable who's parameter are tags and returns
        a str.  ie. ``out_dir=lambda tags: '{color}/' if tags['has_color'] else 'square/'``
    :yields: Tasks.
    """
    execution = parents[0].execution

    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for new_tags, parent_group in group(parents, groupby):
            new_tags.update(tag)
            yield execution.add_task(cmd_fxn, tags=new_tags, parents=parent_group,
                                     out_dir=out_dir(new_tags) if hasattr(out_dir, '__call__') else out_dir,
                                     stage_name=stage_name
                                     )

    return list(g())


def combinations(splitby):
    for items in it.product(*[[(k, v) for v in l] for k, l in splitby.items()]):
        yield dict(items)


def one2many(cmd_fxn, parents, splitby, tag=None, out_dir='', stage_name=None):
    """
    :param dict splitby: a dict who's values are lists, ex: dict(color=['red','blue'], shape=['square','circle'])
    :yields: Tasks.
    """
    execution = parents[0].execution

    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    def g():
        for parent_task in parents:
            new_tags = parent_task.tags.copy()
            tag_itrbl = splitby(parent_task) if hasattr(splitby,'__call__') else combinations(splitby)

            for split_tags in tag_itrbl:
                new_tags.update(split_tags)
                new_tags.update(tag)
                yield execution.add_task(cmd_fxn, tags=new_tags, parents=[parent_task],
                                         out_dir=out_dir(new_tags) if hasattr(out_dir, '__call__') else out_dir,
                                         stage_name=stage_name)

    return list(g())
