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
    :param tool_class: (Tool) a subclass of Tool to create new tasks with.
    :param tasks: ([Task, ...]) A child tool will be created for each element in this list.
    :param tag: (dict) Tags to add to the Tools's dictionary.  The Tool will also inherit the tags of its parent.
        Defaults to the tags of the parent
    :param out: (str) The directory to output to, will be .formated() with its task's tags.  ex. '{shape}/{color}'.
        Defaults to the out of the parent task.
    :return: ([Tool, ...]) A list of tools that were added.
    """
    if tag is None:
        tag = dict()

    assert isinstance(tag, dict), '`tag` must be a dict'
    for parent in tasks:
        new_tags = parent.tags.copy()
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent, out=out or parent.output_dir)


def many2one(tool_class, parents, group_keys, tag=None, out=''):
    if tag is None:
        tag = dict()
    assert isinstance(tag, dict), '`tag` must be a dict'
    parents = list(parents)
    assert all(isinstance(t, Task) for t in parents), '`parents` must be an iterable of Tasks'

    def f(task):
        return {k:v for k, v in task.tags.items() if k in group_keys}

    for tag_group, parent_group in it.groupby(sorted(parents, key=f), f):
        new_tags = dict(tag_group)
        new_tags.update(tag)
        yield tool_class(tags=new_tags, parents=parent_group, out=out)

