from recordtype import recordtype
import operator
from collections import OrderedDict
import re
from inspect import getargspec
import re
import itertools as it
import os

import funcsigs

FindFromParents = recordtype('FindFromParents', 'regex n tags', default=None)
OutputDir = recordtype('OutputDir', 'basename prepend_execution_output_dir', default=None)
Forward = recordtype('Forward', 'input_parameter_name', default=None)


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


def find(regex, n='==1', tags=None):
    """
    Used to set an input_file's default behavior to finds output_files from a Task's parents using a regex

    :param str regex: a regex to match the file path
    :param str n: (cardinality) the number of files to find
    :param dict tags: filter parent search space using these tags
    """
    return FindFromParents(regex, n, tags)


def out_dir(basename='', peo=True):
    """
    Essentially will perform os.path.join(Task.output_dir, basename)

    :param str basename: The basename of the output_file
    :param bool peo: Prepend execution.output_dir to the output
    """
    return OutputDir(basename, peo)


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
        import sys

        print >> sys.stderr
        print >> sys.stderr, '<ERROR msg="{cmd_name}() does not have right number of inputs for {find_instance}"'.format(**locals())
        for parent in parents:
            print >> sys.stderr, '\t<PARENT task="%s">' % parent
            if len(parent.output_files):
                for out_file in parent.output_files:
                    print >> sys.stderr, '\t\t<OUTPUT_FILE path="%s" match=%s />' % (out_file, out_file in mapped_input_taskfiles)
            print >> sys.stderr, '\t</PARENT>'
        print >> sys.stderr, '</ERROR>'

        raise ValueError('Input files are missing, or their cardinality do not match.')


def _get_input_map(cmd_name, cmd_fxn, tags, parents):
    # todo handle inputs without default

    sig = funcsigs.signature(cmd_fxn)

    # funcsigs._empty
    for param_name, param in sig.parameters.iteritems():
        if param_name.startswith('in_'):
            if param_name in tags:
                # user specified explicitly
                input_file = tags[param_name]
                yield param_name, input_file
            elif isinstance(param.default, FindFromParents):
                # user used find()
                find_instance = param.default

                def get_available_files():
                    for p in parents:
                        if all(p.tags.get(k) == v for k, v in (find_instance.tags or dict()).items()):
                            yield p.output_files

                available_files = it.chain(*get_available_files())
                input_taskfiles = list(_find(available_files, find_instance.regex, error_if_missing=False))
                _validate_input_mapping(cmd_name, find_instance, input_taskfiles, parents)
                input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(find_instance, input_taskfiles)

                yield param_name, input_taskfile_or_input_taskfiles
            else:
                raise AssertionError, '%s Bad input `%s`, with default `%s`.  Set its default to find(), or specify ' \
                                      'its value via tags' % (cmd_name, param_name, param.default)


def _get_output_map(stage_name, cmd_fxn, tags, input_map, output_dir, execution_output_dir):
    sig = funcsigs.signature(cmd_fxn)

    for param_name, param in sig.parameters.iteritems():

        if param_name not in tags and param.default is funcsigs._empty:
            raise ValueError('Required output file parameter `%s` not specified for %s.' % (param_name, stage_name))

        value = tags.get(param_name, param.default)

        if isinstance(value, Forward):
            forward_instance = value
            try:
                input_value = input_map[forward_instance.input_parameter_name]
            except KeyError:
                raise KeyError('Cannot forward name `%s`,it is not a valid input parameter of '
                               '%s in stage %s' % (forward_instance.input_parameter_name, cmd_fxn, stage_name))
            yield param_name, input_value
        elif isinstance(value, OutputDir):
            output_dir_instance = value
            if output_dir is not None:
                if output_dir_instance.prepend_execution_output_dir:
                    output_dir = os.path.join(execution_output_dir, output_dir)
                output_file = os.path.join(output_dir, output_dir_instance.basename.format(**tags))
            else:
                output_file = output_dir_instance.basename.format(**tags)
            # output_file = value.format(**tags)
            yield param_name, output_file
        elif param_name.startswith('out_'):
            yield param_name, value



def get_io_map(fxn, tags, parents, cmd_name, output_dir, execution_output_dir):
    # input_arg_to_default, output_arg_to_default = get_input_and_output_defaults(fxn)
    input_map = dict(_get_input_map(cmd_name, fxn, tags, parents))
    output_map = dict(_get_output_map(cmd_name, fxn, tags, input_map, output_dir, execution_output_dir))

    return input_map, output_map


def unpack_io_map(io_map):
    return list(it.chain(*(v if isinstance(v, list) else [v] for v in io_map.values())))
