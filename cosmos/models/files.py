# class FindFromParents(object):
from collections import namedtuple


def find_deprecated(name, format, n):
    return find(name + '.' + format, n)


find = namedtuple('FindFromParents', 'regex n')
out_dir = namedtuple('OutputDir', 'basename')
forward = namedtuple('Forward', 'input_name')

