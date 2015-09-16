from recordtype import recordtype
import operator
from collections import OrderedDict
import re
from inspect import getargspec
import re
import itertools as it
import os


def get_input_and_output_defaults(fxn):
    argspec = getargspec(fxn)
    input_arg_to_default = dict()
    output_arg_to_default = dict()

    # iterate over argspec keywords and their defaults
    num_no_default = len(argspec.args) - len(argspec.defaults or [])
    for kw, default in zip(argspec.args, [None] * num_no_default + list(argspec.defaults or [])):
        # if isinstance(kw, list):
        # # for when user specifies unpacking in a parameter name
        # kw = frozenset(kw)
        if kw.startswith('in_') or isinstance(default, FindFromParents):
            input_arg_to_default[kw] = default
        elif kw.startswith('out_') or isinstance(default, OutputDir) or isinstance(default, Forward):
            output_arg_to_default[kw] = default

    return input_arg_to_default, output_arg_to_default


def unpack_if_cardinality_1(find_instance, taskfiles):
    op, number = parse_cardinality(find_instance.n)
    if op in ['=', '=='] and number == 1:
        return taskfiles[0]
    else:
        return taskfiles


def _find(filenames, regex, error_if_missing=False):
    found = False
    for filename in filenames:
        if re.search(regex, filename):
            yield filename
            found = True

    if not found and error_if_missing:
        raise ValueError, 'No taskfile found for %s' % regex


OPS = OrderedDict([("<=", operator.le),
                   ("<", operator.lt),
                   (">=", operator.ge),
                   (">", operator.gt),
                   ('==', operator.eq),
                   ("=", operator.eq)])


def parse_cardinality(n):
    try:
        op, number = re.search('(.*?)(\d+)', str(n)).groups()
    except AttributeError:
        raise AttributeError('Invalid cardinality: %s' % n)
    if op == '':
        op = '=='
    number = int(number)
    return op, number


FindFromParents = recordtype('FindFromParents', 'regex n tags', default=None)
OutputDir = recordtype('OutputDir', 'basename', default=None)
Forward = recordtype('Forward', 'input_parameter_name', default=None)


def find(regex, n='==1', tags=None):
    """
    Used to set an input_file's default behavior to finds output_files from a Task's parents using a regex

    :param str regex: a regex to match the file path
    :param str n: (cardinality) the number of files to find
    :param dict tags: filter parent search space using these tags
    """
    return FindFromParents(regex, n, tags)


def out_dir(basename=''):
    """
    Essentially will perform os.path.join(Task.output_dir, basename)

    :param str basename: The basename of the output_file
    """
    return OutputDir(basename)


def forward(input_parameter_name):
    """
    Forwards a Task's input as an output

    :param input_parameter_name: The name of this cmd_fxn's input parameter to forward
    """
    return Forward(input_parameter_name)


def _validate_input_mapping(cmd_name, find_instance, mapped_input_taskfiles, parents):
    real_count = len(mapped_input_taskfiles)
    op, number = parse_cardinality(find_instance.n)

    if not OPS[op](real_count, int(number)):
        s = '******ERROR****** \n' \
            '{cmd_name} does not have right number of inputs: for {find_instance}\n' \
            '***Parents*** \n' \
            '{prnts}\n' \
            '***Inputs Matched ({real_count})*** \n' \
            '{mit} '.format(mit="\n".join(map(str, mapped_input_taskfiles)),
                            prnts="\n".join(map(str, parents)), **locals())
        import sys

        print >> sys.stderr, s
        raise ValueError('Input files are missing, or their cardinality do not match.')


def _get_input_map(cmd_name, input_arg_to_default, tags, parents):
    # todo handle inputs without default

    for input_name, input_value in input_arg_to_default.iteritems():
        if input_name in tags:
            # user specified explicitly
            input_file = tags[input_name]
            yield input_name, input_file
        elif isinstance(input_value, FindFromParents):
            # user used find()
            find_instance = input_value

            def get_available_files():
                for p in parents:
                    if all(p.tags.get(k) == v for k,v in (find_instance.tags or dict()).items()):
                        yield p.output_files

            available_files = it.chain(*get_available_files())
            input_taskfiles = list(_find(available_files, find_instance.regex, error_if_missing=False))
            _validate_input_mapping(cmd_name, find_instance, input_taskfiles, parents)
            input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(find_instance, input_taskfiles)

            yield input_name, input_taskfile_or_input_taskfiles
        else:
            raise AssertionError, '%s Bad input `%s`, with default `%s`.  Set its default to find(), or specify ' \
                                  'its value via tags' % (cmd_name, input_name, input_value)


def _get_output_map(cmd_name, output_arg_to_default, tags, input_map, output_dir):
    for name, value in output_arg_to_default.iteritems():
        if name in tags:
            output_file = tags[name]
            yield name, output_file

        elif isinstance(value, Forward):
            try:
                input_value = input_map[value.input_parameter_name]
            except KeyError:
                raise KeyError('Cannot forward name `%s`,it is not a valid input parameter of '
                               '%s.cmd()' % (value.input_parameter_name, cmd_name))
            yield name, input_value
        elif isinstance(value, OutputDir):
            if output_dir is not None:
                output_file = os.path.join(output_dir,
                                           value.basename.format(**tags))
            else:
                output_file = value.basename.format(**tags)
            # output_file = value.format(**tags)
            yield name, output_file
        else:
            print name, value, tags
            yield name, value.format(**tags)


def get_io_map(fxn, tags, parents, cmd_name, output_dir):
    input_arg_to_default, output_arg_to_default = get_input_and_output_defaults(fxn)
    input_map = dict(_get_input_map(cmd_name, input_arg_to_default, tags, parents))
    output_map = dict(_get_output_map(cmd_name, output_arg_to_default, tags, input_map, output_dir))

    return input_map, output_map


def unpack_io_map(io_map):
    return list(it.chain(*(v if isinstance(v, list) else [v] for v in io_map.values())))