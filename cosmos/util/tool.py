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


def many2one(tool_class, parents, group_keys, tag=None, out=''):
    """
    :param cosmos.Tool tool_class: a subclass of Tool to create new tasks with.
    :param list(str) group_keys: A list of keys to groupby.  Parents will be grouped if they have the same values in
        their tags given by `group_keys`.
    :param itrbl(Task) parents: An group of parents to groupby
    :param dict tag: Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
    :param str out: The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the output_dir of the parent task.
    :return:
    """
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'
    parents = list(parents)
    assert all(isinstance(t, Task) for t in parents), '`parents` must be an iterable of Tasks'

    def f(task):
        return {k: v for k, v in task.tags.items() if k in group_keys}

    for tag_group, parent_group in it.groupby(sorted(parents, key=f), f):
        new_tags = dict(tag_group)
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent_group, out=out)


def many2many():
    """
    TODO
    :return:
    """
    raise NotImplementedError()


def one2many():
    """
    TODO
    :return:
    """
    raise NotImplementedError()
