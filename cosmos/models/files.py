# class FindFromParents(object):
from recordtype import recordtype


# for reverse compatibility
def abstract_input_taskfile(name=None, format=None, n=1):
    return find(name or '' + '.' + format or '', n)

def abstract_output_taskfile(basename=None, name=None, format=None):
    if not basename:
        basename = name or '' + '.' + format or ''
    return out_dir(basename)

def abstract_output_taskfile_old(name=None, format=None, basename=None):
    if not basename:
        basename = name or '' + '.' + format or ''
    return out_dir(basename)

find = recordtype('FindFromParents', 'regex n tags', default=None)
out_dir = recordtype('OutputDir', 'basename', default=None)
forward = recordtype('Forward', 'input_parameter_name', default=None)