# class FindFromParents(object):
from collections import namedtuple


# for reverse compatibility
def input_abstract_file(name=None, format=None, n=1):
    return find(name or '' + '.' + format or '', n)

def output_abstract_file(basename, name=None, format=None, n=None):
    if not basename:
        basename = name or '' + '.' + format or ''
    return out_dir(basename, n)

find = namedtuple('FindFromParents', 'regex n')
out_dir = namedtuple('OutputDir', 'basename')
forward = namedtuple('Forward', 'input_name')