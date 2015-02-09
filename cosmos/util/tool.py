import re
import itertools as it
from ..util.helpers import groupby2
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


def one2one(tool_class, parent_groups, tag=None, out=''):
    if tag is None:
        tag = dict()
    for parent_group in parent_groups:
        if isinstance(parent_group, Task):
            parent_group = [parent_group]
        yield tool_class(tags=make_dict(*parent_group, **tag), parents=parent_group, out=out)