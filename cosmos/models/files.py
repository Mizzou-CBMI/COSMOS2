"""
DEPRECATED
"""

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


def find(regex, n='==1', tags=None):
    """
    Used to set an input_file's default behavior to finds output_files from a Task's parents using a regex

    :param str regex: a regex to match the file path
    :param str n: (cardinality) the number of files to find
    :param dict tags: filter parent search space using these tags
    """
    return recordtype('FindFromParents', 'regex n tags', default=None)


def out_dir(basename=''):
    """
    Essentially will perform os.path.join(Task.output_dir, basename)

    :param str basename: The basename of the output_file
    """
    return recordtype('OutputDir', 'basename', default=None)


def forward(input_parameter_name):
    """
    Forwards a Task's input as an output

    :param input_parameter_name: The name of this cmd_fxn's input parameter to forward
    """
    return recordtype('Forward', 'input_parameter_name', default=None)