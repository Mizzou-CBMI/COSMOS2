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
        if kw.startswith('in_') or isinstance(default, find):
            input_arg_to_default[kw] = default
        elif kw.startswith('out_') or isinstance(default, out_dir) or isinstance(default, forward):
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
    op, number = re.search('(.*?)(\d+)', str(n)).groups()
    if op == '':
        op = '=='
    number = int(number)
    return op, number


find = recordtype('FindFromParents', 'regex n tags', default=None)
out_dir = recordtype('OutputDir', 'basename', default=None)
forward = recordtype('Forward', 'input_parameter_name', default=None)


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


def _get_input_map(fxn, input_arg_to_default, tags, parents, cmd_name=None):
    # if cmd_name is None:
    #     cmd_name = str(fxn)

    # todo handle inputs without default

    for input_name, input_value in input_arg_to_default.iteritems():
        if input_name in tags:
            # user specified explicitly
            input_file = tags[input_name]
            yield input_name, input_file
        elif isinstance(input_value, find):
            # user used find()
            find_instance = input_value
            available_files = it.chain(*(p.output_files for p in parents))
            input_taskfiles = list(_find(available_files, find_instance.regex, error_if_missing=False))
            _validate_input_mapping(cmd_name, find_instance, input_taskfiles, parents)
            input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(find_instance, input_taskfiles)

            yield input_name, input_taskfile_or_input_taskfiles
        else:
            raise AssertionError, '%s Bad input `%s`, with default `%s`.  Set its default to find(), or specify' \
                                  'its value via tags' % (str(fxn), input_name, input_value)


def _get_output_map(output_arg_to_default, tags):
    for name, value in output_arg_to_default.iteritems():
        if name in tags:
            output_file = tags[name]
            yield name, output_file

        elif isinstance(value, forward):
            try:
                input_value = input_map[value.input_parameter_name]
            except KeyError:
                raise KeyError('Cannot forward name `%s`,it is not a valid input parameter of '
                               '%s.cmd()' % (value.input_parameter_name, self.name))
            yield name, input_value
        elif isinstance(value, out_dir):
            output_file = os.path.join(self.output_dir, value.basename.format(**self.tags))
            yield name, output_file
        else:
            yield name, value


def get_io_map(fxn, tags, parents):
    input_arg_to_default, output_arg_to_default = get_input_and_output_defaults(fxn)


### Deprecated

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