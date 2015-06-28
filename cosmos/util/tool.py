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


def reduce_(parents, by):
    """
    helpers for many2one and many2many
    """
    parents = list(parents)
    assert all(isinstance(t, Task) for t in parents), '`parents` must be an iterable of Tasks'


    def f(task):
        try:
            return {k: task.tags[k] for k in by}
        except KeyError as k:
            raise KeyError('keyword %s is not in the tags of %s' % (k, task))


    for group_tags, parent_group in it.groupby(sorted(parents, key=f), f):
        yield group_tags.copy(), parent_group


def many2one(tool_class, parents, groupby, tag=None, out=''):
    """
    :param cosmos.Tool tool_class: a subclass of Tool to create new tasks with.
    :param list(str) groupby: A list of keys to groupby.  Parents will be grouped if they have the same values in
        their tags given by `groupby`.
    :param itrbl(Task) parents: An group of parents to groupby
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str|callable out: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.  Alternatively use a callable who's parameter are tags and returns
        a str.  ie. ``out=lambda tags: '{color}/' if tags['has_color'] else 'square/'``
    :yields: new Tools
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    for new_tags, parent_group in reduce_(parents, groupby):
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent_group, out=out(new_tags) if hasattr(out, '__call__') else out)


def many2many(tool_class, parents, groupby, splitby, tag=None, out=''):
    """
    :param dict splitby: a dict who's values are lists, ex: dict(color=['red','blue'], shape=['square','circle'])
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'

    for group_tags, parent_group in reduce_(parents, groupby):
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
