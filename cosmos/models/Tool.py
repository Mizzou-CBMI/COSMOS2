from inspect import getargspec
import os
import re
import itertools as it
import operator
from collections import OrderedDict
import types

from .. import Task, NOOP
# from ..models.TaskFile import InputFileAssociation, AbstractInputFile, AbstractOutputFile
from ..util.helpers import str_format, has_duplicates, strip_lines
from .files import find, forward, out_dir


class ToolValidationError(Exception): pass


opj = os.path.join

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


class _ToolMeta(type):
    def __init__(cls, name, bases, dct):
        cls.name = name
        return super(_ToolMeta, cls).__init__(name, bases, dct)


def error(message, dct):
    import sys

    print >> sys.stderr, '******ERROR*******'
    print >> sys.stderr, '    %s' % message
    for k, v in dct.items():
        print >> sys.stderr, '***%s***' % k
        if isinstance(v, list):
            for v2 in v:
                print >> sys.stderr, "    %s" % v2
        elif isinstance(v, dict):
            for k2, v2 in v.items():
                print >> sys.stderr, "    %s = %s" % (k2, v2)
        else:
            print >> sys.stderr, "    %s" % v

    raise ToolValidationError(message)


class Tool(object):
    """
    Essentially a factory that produces Tasks.  :meth:`cmd` must be overridden unless it is a NOOP task.
    """
    __metaclass__ = _ToolMeta

    mem_req = None
    time_req = None
    cpu_req = None
    must_succeed = True
    # NOOP = False
    persist = False
    drm = None
    output_dir = None

    def __init__(self, tags, parents=None, out=''):
        """
        :param tags: (dict) A dictionary of tags.  The combination of tags are the unique identifier for a Task,
            and must be unique for any tasks in its stage.  They are also passed as parameters to the cmd() call.  Tag
            values must be basic python types.
        :param parents: (list of Tasks).  A list of parent tasks
        :param out: an output directory, will be .format()ed with tags.  Defaults to the cwd of the Execution.
        """
        assert isinstance(tags, dict), '`tags` must be a dict'
        assert isinstance(out, basestring), '`out_dir` must be a str'
        if isinstance(parents, types.GeneratorType):
            parents = list(parents)
        if parents is None:
            parents = []

        if issubclass(parents.__class__, Task):
            parents = [parents]
        else:
            parents = list(parents)

        assert hasattr(parents, '__iter__'), 'Tried to set %s.parents to %s which is not iterable' % (self, parents)
        assert all(issubclass(p.__class__, Task) for p in parents), 'parents must be an iterable of Tasks or a Task'

        parents = filter(lambda p: p is not None, parents)

        self.tags = tags.copy()  # can't expect the User to remember to do this.
        self.__validate()
        self.load_sources = []  # for Inputs

        self.out = out
        self.task_parents = parents if parents else []

        argspec = getargspec(self.cmd)
        self.input_arg_to_default = dict()
        self.output_arg_to_default = dict()

        # iterate over argspec keywords and their defaults
        num_no_default = len(argspec.args)-len(argspec.defaults or [])
        for kw, default in zip(argspec.args,[None]*num_no_default + list(argspec.defaults or [])):
            if isinstance(kw, list):
                # for when user specifies unpacking in a parameter name
                kw = frozenset(kw)
            if kw.startswith('in_') or isinstance(default, find):
                self.input_arg_to_default[kw] = default
            elif kw.startswith('out_') or isinstance(default, out_dir) or isinstance(default, forward):
                self.output_arg_to_default[kw] = default

    def __validate(self):
        # assert all(i.__class__.__name__ == 'AbstractInputFile' for i in
        # self.abstract_inputs), '%s Tool.abstract_inputs must be of type AbstractInputFile' % self
        # assert all(o.__class__.__name__ == 'AbstractOutputFile' for o in
        #            self.abstract_outputs), '%s Tool.abstract_outputs must be of type AbstractOutputFile' % self

        # if has_duplicates([(i.name, i.format) for i in self.abstract_inputs]):
        #     raise ToolValidationError("Duplicate task.abstract_inputs detected in {0}".format(self))
        #
        # if has_duplicates([(i.name, i.format) for i in self.abstract_outputs]):
        #     raise ToolValidationError("Duplicate task.abstract_outputs detected in {0}".format(self))

        # reserved = {'name', 'format', 'basename'}
        # if not set(self.tags.keys()).isdisjoint(reserved):
        #     raise ToolValidationError(
        #         "%s are a reserved names, and cannot be used as a tag keyword in %s" % (reserved, self))

        from cosmos import ERROR_IF_TAG_IS_NOT_BASIC_TYPE

        if ERROR_IF_TAG_IS_NOT_BASIC_TYPE:
            for k, v in self.tags.iteritems():
                # msg = '%s.tags[%s] is not a basic python type.  ' \
                #       'Tag values should be a str, int, float or bool.' \
                #       'Alternatively, you can set cosmos.ERROR_OF_TAG_IS_NOT_BASIC_TYPE = False. \'' \
                #       'IF YOU ENABLE THIS, TAGS THAT ARE NOT BASIC TYPES WILL ONLY BE USED AS PARAMETERS TO THE cmd()' \
                #       'FUNCTION, AND NOT FOR MATCHING PREVIOUSLY SUCCESSFUL TASKS WHEN RESUMING OR STORED IN THE' \
                #       'SQL DB.' % (self,k)
                msg = '%s.tags[%s] is not a basic python type.  ' \
                      'Tag values should be a str, int, float or bool.' % (self, k)
                assert any(isinstance(v, t) for t in [basestring, int, float, bool]), msg


    def _validate_input_mapping(self, find_instance, mapped_input_taskfiles, parents):
        real_count = len(mapped_input_taskfiles)
        op, number = parse_cardinality(find_instance.n)

        if not OPS[op](real_count, int(number)):
            s = '******ERROR****** \n' \
                '{self} does not have right number of inputs: for {find_instance}\n' \
                '***Parents*** \n' \
                '{prnts}\n' \
                '***Inputs Matched ({real_count})*** \n' \
                '{mit} '.format(mit="\n".join(map(str, mapped_input_taskfiles)),
                                prnts="\n".join(map(str, parents)), **locals())
            import sys

            print >> sys.stderr, s
            raise ToolValidationError('Input files are missing, or their cardinality do not match.')

    def _generate_task(self, stage, parents, default_drm):
        assert self.out is not None
        self.output_dir = str_format(self.out, self.tags, '%s.output_dir' % self)
        # self.output_dir = os.path.join(stage.execution.output_dir, self.output_dir)
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed']}
        d['drm'] = 'local' if self.drm is not None else default_drm

        # Validation
        # f = lambda ifa: ifa.taskfile
        # for tf, group_of_ifas in it.groupby(sorted(ifas, key=f), f):
        # group_of_ifas = list(group_of_ifas)
        #     if len(group_of_ifas) > 1:
        #         error('An input file mapped to multiple AbstractInputFiles for %s' % self, dict(
        #             TaskFiles=tf
        #         ))

        def get_input_map():
            for input_name, input_value in self.input_arg_to_default.iteritems():
                if input_name in self.tags:
                    # user specified explicitly
                    input_file = self.tags[input_name]
                    yield input_name, input_file
                elif isinstance(input_value, find):
                    # user used find()
                    find_instance = input_value
                    available_files = it.chain(*(p.output_files for p in parents))
                    input_taskfiles = list(_find(available_files, find_instance.regex, error_if_missing=False))
                    self._validate_input_mapping(find_instance, input_taskfiles, parents)
                    input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(find_instance, input_taskfiles)

                    yield input_name, input_taskfile_or_input_taskfiles
                else:
                    raise AssertionError, '%s Bad input `%s`, with default `%s`.  Set its default to find(), or specify' \
                                          'its value via tags' % (self, input_name, input_value)

        def get_output_map():
            for name, value in self.output_arg_to_default.iteritems():
                if name in self.tags:
                    output_file = self.tags[name]
                    yield name, output_file

                elif isinstance(value, forward):
                    try:
                        input_value = self.input_map[value.input_parameter_name]
                    except KeyError:
                        raise KeyError('Cannot forward name `%s`,it is not a valid input parameter of '
                                       '%s.cmd()' % (value.input_parameter_name, self.name))
                    yield name, input_value
                elif isinstance(value, out_dir):
                    output_file = os.path.join(self.output_dir, value.basename.format(**self.tags))
                    yield name, output_file
                else:
                    yield name, value


            # for Input() and Inputs() nodes
            for (path, name, format) in self.load_sources:
                yield name, path

        self.input_map = dict(get_input_map())
        self.output_map = dict(get_output_map())

        input_files = list(it.chain(*(v if isinstance(v, list) else [v] for v in self.input_map.values())))
        output_files = list(it.chain(*(v if isinstance(v, list) else [v] for v in self.output_map.values())))


        task = Task(stage=stage, tags=self.tags, parents=parents, output_dir=self.output_dir, input_files=input_files,
                    output_files=output_files, **d)

        # inputs = unpack_taskfiles_with_cardinality_1(aif_2_input_taskfiles).values()

        task.tool = self
        return task

    def _cmd(self, task):
        argspec = getargspec(self.cmd)
        self.task = task

        def get_params():
            for k in argspec.args:
                if k in self.tags:
                    yield k, self.tags[k]

        params = dict(get_params())

        def validate_params():
            ndefaults = len(argspec.defaults) if argspec.defaults else 0
            for arg in argspec.args[1:-1 * ndefaults]:
                if arg not in params:
                    raise AttributeError(
                        '%s.cmd() requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                        'method signature, or pass a value for `%s` parameter using a tag' % (self, arg, arg))

        validate_params()



        kwargs = dict()
        kwargs.update(self.input_map)
        kwargs.update(self.output_map)
        kwargs.update(params)

        out = self.cmd(**kwargs)

        assert isinstance(out, basestring), '%s.cmd did not return a str' % self
        out = re.sub('<TaskFile\[(.*?)\] .+?:(.+?)>', lambda m: m.group(2), out)
        return out  # strip_lines(out_dir)

    def before_cmd(self):
        task = self.task
        o = '#!/bin/bash\n' \
            'set -e\n' \
            'set -o pipefail\n' \
            'cd %s\n' % task.execution.output_dir

        if task.output_dir:
            o += 'mkdir -p %s\n' % task.output_dir

        o += "\n"

        return o

    def after_cmd(self):
        return ''

    def cmd(self, **kwargs):
        """
        Constructs the command string.  Lines will be .strip()ed.
        :param dict kwargs:  Inputs and Outputs (which have AbstractInputFile and AbstractOutputFile defaults) and parameters which are passed via tags.
        :rtype: str
        :returns: The text to write into the shell script that gets executed
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def _generate_command(self, task):
        """
        Generates the command
        """
        cmd = self._cmd(task)
        if cmd == NOOP:
            return NOOP
        return self.before_cmd() + cmd + self.after_cmd()

    def __repr__(self):
        return '<Tool[%s] %s %s>' % (id(self), self.name, self.tags)


from collections import namedtuple


class InputSource(namedtuple('InputSource', ['path', 'name', 'format'])):
    def __init__(self, path, name=None, format=None):
        basename = os.path.basename(path)
        if name is None:
            name = os.path.splitext(basename)[0]
        if format is None:
            format = os.path.splitext(basename)[-1][1:]  # remove the '.'

        super(InputSource, self).__init__(path, name, format)


def set_default_name_format(path, name=None, format=None):
    default_name, default_ext = os.path.splitext(os.path.basename(path))

    if name is None:
        name = default_name
    if format is None:
        format = default_ext[1:]

    return name, format


class Input(Tool):
    """
    A NOOP Task who's output_files contain a *single* file that already exists on the filesystem.
    >>> Input(path_to_file,tags={'key':'val'})
    >>> Input(path=path_to_file, name='myfile',format='txt',tags={'key':'val'})
    """

    name = 'Load_Input_Files'
    cpu_req = 0

    def __init__(self, path, name=None, format=None, tags=None, *args, **kwargs):
        """
        :param str path: the path to the input file
        :param str name: the name or keyword for the input file.  defaults to whatever format is set to.
        :param str format: the format of the input file.  Defaults to the value in `name`
        :param dict tags: tags for the task that will be generated
        """

        # path = _abs(path)
        if tags is None:
            tags = dict()

        name, format = set_default_name_format(path, name, format)

        super(Input, self).__init__(tags=tags, *args, **kwargs)
        self.load_sources.append(InputSource(path, name, format))

    def cmd(self, *args, **kwargs):
        return NOOP


class Inputs(Tool):
    """
    Same as :class:`Input`, but loads multiple input files.
    >>> Inputs([('name1','txt','/path/to/input'), ('name2','gz','/path/to/input2')], tags={'key':'val'})
    "root_path   name = 'Load_Input_Files'
    """
    name = 'Load_Input_Files'
    cpu_req = 0

    def __init__(self, inputs, tags=None, *args, **kwargs):
        """
        :param list inputs: a list of tuples that are (path, name, format)
        :param dict tags:
        """
        # self.NOOP = True
        if tags is None:
            tags = dict()

        super(Inputs, self).__init__(tags=tags, *args, **kwargs)
        for path, name, fmt in inputs:
            name, fmt = set_default_name_format(path, name, fmt)
            self.load_sources.append(InputSource(path, name, fmt))

    def cmd(self, *args, **kwargs):
        return NOOP


def _abs(path):
    path2 = os.path.abspath(os.path.expanduser(path))
    assert os.path.exists(path2), '%s path does not exist' % path2
    return path2


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