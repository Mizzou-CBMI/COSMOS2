from inspect import getargspec
import os
import re
import itertools as it
import operator
from collections import OrderedDict

from .. import TaskFile, Task, NOOP
from ..models.TaskFile import InputFileAssociation, AbstractInputFile, AbstractOutputFile
from ..util.iterstuff import only_one
from ..util.helpers import str_format, groupby2, has_duplicates, strip_lines, isgenerator
import types


class ToolValidationError(Exception): pass


opj = os.path.join

OPS = OrderedDict([("<=", operator.le),
                   ("<", operator.lt),
                   (">=", operator.ge),
                   (">", operator.gt),
                   ('==', operator.eq),
                   ("=", operator.eq)])


def parse_aif_cardinality(n):
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
    skip_profile = False
    inputs = []  # class property!
    outputs = []  # class property!
    output_dir = None
    api_version = 2

    # if adding another attribute, don't forget to update the chain() method


    def __init__(self, tags, parents=None, out=''):
        """
        :param tags: (dict) A dictionary of tags.
        :param parents: (list of Tasks).  A list of parent tasks
        :param out: an output directory, will be .format()ed with tags
        """
        assert isinstance(tags, dict), '`tags` must be a dict'
        assert isinstance(out, basestring), '`out` must be a str'
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

        if self.api_version == 2:
            argspec = getargspec(self.cmd)
            self.input_arg_map = {}
            self.output_arg_map = {}

            # iterate over argspec keywords and their defaults
            for kw, default in zip(argspec.args[-len(argspec.defaults or []):], argspec.defaults or []):
                if isinstance(kw, list):
                    # for when user specifies unpacking in a parameter name
                    kw = frozenset(kw)

                if isinstance(default, AbstractInputFile):
                    self.input_arg_map[kw] = default
                elif isinstance(default, AbstractOutputFile):
                    self.output_arg_map[kw] = default

            self.inputs = self.input_arg_map.values()
            self.outputs = self.output_arg_map.values()


    def __validate(self):
        assert all(i.__class__.__name__ == 'AbstractInputFile' for i in
                   self.inputs), '%s Tool.inputs must be of type AbstractInputFile' % self
        assert all(o.__class__.__name__ == 'AbstractOutputFile' for o in
                   self.outputs), '%s Tool.outputs must be of type AbstractOutputFile' % self

        if has_duplicates([(i.name, i.format) for i in self.inputs]):
            raise ToolValidationError("Duplicate task.inputs detected in {0}".format(self))

        if has_duplicates([(i.name, i.format) for i in self.outputs]):
            raise ToolValidationError("Duplicate task.outputs detected in {0}".format(self))

        if self.api_version == 1:
            argspec = getargspec(self.cmd)
            if isinstance(argspec.args[1], list):
                assert len(argspec.args[1]) == len(self.inputs), '%s.cmd will not unpack its inputs correctly.' % self
            if isinstance(argspec.args[2], list):
                assert len(argspec.args[2]) == len(self.outputs), '%s.cmd will not unpack its outputs correctly' % self

        reserved = {'name', 'format', 'basename'}
        if not set(self.tags.keys()).isdisjoint(reserved):
            raise ToolValidationError(
                "%s are a reserved names, and cannot be used as a tag keyword in %s" % (reserved, self))

        for v in self.tags.itervalues():
            assert any(
                isinstance(v, t) for t in [basestring, int, float, bool]), '%s.tags[%s] is not a basic python type.  ' \
                                                                           'Tag values should be a str, int, float or bool.'


    def _validate_input_mapping(self, abstract_input_file, mapped_input_taskfiles, parents):
        real_count = len(mapped_input_taskfiles)
        op, number = parse_aif_cardinality(abstract_input_file.n)
        import pprint

        if not OPS[op](real_count, int(number)):
            s = '******ERROR****** \n' \
                '{self} does not have right number of inputs: for {abstract_input_file}\n' \
                '***Parents*** \n' \
                '{prnts}\n' \
                '***Inputs Matched ({real_count})*** \n' \
                '{mit} '.format(mit="\n".join(map(str, mapped_input_taskfiles)),
                                prnts="\n".join(map(str, parents)), **locals())
            import sys

            print >> sys.stderr, s
            raise ToolValidationError('Input files are missing, or their cardinality do not match.')


    def _map_inputs(self, parents):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: [(taskfile, is_forward), ...]
        """
        for aif_index, abstract_input_file in enumerate(self.inputs):
            mapped_input_taskfiles = list(set(self._map_input(abstract_input_file, parents)))
            self._validate_input_mapping(abstract_input_file, mapped_input_taskfiles, parents)
            yield abstract_input_file, mapped_input_taskfiles


    def _map_input(self, abstract_input_file, parents):
        for p in parents:
            for tf in _find(p.output_files + p.forwarded_inputs, abstract_input_file, error_if_missing=False):
                yield tf

    def _generate_task(self, stage, parents, default_drm):
        assert self.out is not None
        self.output_dir = str_format(self.out, self.tags, '%s.output_dir' % self)
        self.output_dir = os.path.join(stage.execution.output_dir, self.output_dir)
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed']}
        d['drm'] = 'local' if self.drm is not None else default_drm

        aif_2_input_taskfiles = OrderedDict(self._map_inputs(parents))

        ifas = [InputFileAssociation(taskfile=tf, forward=aif.forward) for aif, tfs in aif_2_input_taskfiles.items() for
                tf in tfs]

        # Validation
        f = lambda ifa: ifa.taskfile
        for tf, group_of_ifas in it.groupby(sorted(ifas, key=f), f):
            group_of_ifas = list(group_of_ifas)
            if len(group_of_ifas) > 1:
                error('An input file mapped to multiple AbstractInputFiles for %s' % self, dict(
                    TaskFiles=tf
                ))

        task = Task(stage=stage, tags=self.tags, _input_file_assocs=ifas, parents=parents, output_dir=self.output_dir,
                    **d)
        task.skip_profile = self.skip_profile

        inputs = unpack_taskfiles_with_cardinality_1(aif_2_input_taskfiles).values()

        # Create output TaskFiles
        for i, (path, name, format) in enumerate(self.load_sources):
            TaskFile(name=name, format=format, path=path, task_output_for=task, persist=True,
                     basename=os.path.basename(path), order=i, duplicate_ok=True)

        for i, output in enumerate(self.outputs):
            name = str_format(output.name, dict(i=inputs, **self.tags))
            # get basename
            if output.basename is None:
                if output.format == 'dir':
                    basename = output.name
                else:
                    basename = '%s.%s' % (name, output.format)
            else:
                basename = output.basename

            basename = str_format(basename, dict(name=name, format=output.format, i=inputs, **self.tags))
            tf = TaskFile(task_output_for=task, persist=output.persist, name=name, format=output.format,
                          path=opj(self.output_dir, basename), basename=basename, order=i)
            tf.abstract_output_file = output  # for getting sort order when passing to cmd

        task.tool = self
        return task

    def _cmd_v1(self, possible_input_taskfiles, output_taskfiles, task):
        """
        Wrapper for self.cmd().  Passes any tags that match parameter keywords of self.cmd as parameters, and does some basic validation.

        :param output_taskfiles: output TaskFiles in the same order as the AbstractOutputFiles listed in self.outputs
        """

        argspec = getargspec(self.cmd)
        self.task = task
        params = {k: v for k, v in self.tags.items() if k in argspec.args}

        def validate_params():
            ndefaults = len(argspec.defaults) if argspec.defaults else 0
            for arg in argspec.args[3:len(argspec.args) - ndefaults]:
                if arg not in params:
                    raise AttributeError(
                        '%s.cmd() requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                        'method signature, or pass a value for `%s` with a tag' % (self, arg, arg))

        validate_params()

        aif_2_input_taskfiles = OrderedDict((aif, list(_find(possible_input_taskfiles, aif, error_if_missing=True)))
                                            for aif in self.inputs)
        inputs = unpack_taskfiles_with_cardinality_1(aif_2_input_taskfiles).values()
        outputs = sorted(output_taskfiles, key=lambda tf: tf.order)
        out = self.cmd(inputs, outputs, **params)
        assert isinstance(out, basestring), '%s.cmd did not return a str' % self
        out = re.sub('<TaskFile\[(.*?)\] .+?:(.+?)>', lambda m: m.group(2), out)
        return strip_lines(out)

    def _cmd(self, possible_input_taskfiles, output_taskfiles, task):
        if self.api_version == 1:
            return self._cmd_v1(possible_input_taskfiles, output_taskfiles, task)

        argspec = getargspec(self.cmd)
        self.task = task
        params = {k: v for k, v in self.tags.items()
                  if k in argspec.args}

        def validate_params():
            ndefaults = len(argspec.defaults) if argspec.defaults else 0
            for arg in argspec.args[3:len(argspec.args) - ndefaults]:
                if arg not in params:
                    raise AttributeError(
                        '%s.cmd() requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                        'method signature, or pass a value for `%s` with a tag' % (self, arg, arg))

        validate_params()

        def get_input_map():
            for input_name, aif in self.input_arg_map.iteritems():
                input_taskfiles = list(_find(possible_input_taskfiles, aif, error_if_missing=True))
                input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(aif, input_taskfiles)
                yield input_name, input_taskfile_or_input_taskfiles

        input_map = dict(get_input_map())

        outputs = sorted(output_taskfiles, key=lambda tf: tf.order)
        output_map = dict(zip(self.output_arg_map.iterkeys(), outputs))

        kwargs = dict()
        kwargs.update(input_map)
        kwargs.update(output_map)
        kwargs.update(**params)

        out = self.cmd(**kwargs)

        assert isinstance(out, basestring), '%s.cmd did not return a str' % self
        out = re.sub('<TaskFile\[(.*?)\] .+?:(.+?)>', lambda m: m.group(2), out)
        return out  # strip_lines(out)

    def wrap_cmd(self, cmd):
        task = self.task
        return 'OUT={out}\n' \
                'mkdir -p $OUT' \
               'cd $OUT\n\n'.format(out=task.output_dir) + cmd

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
        cmd = self._cmd(task.input_files, task.output_files, task)
        if cmd == NOOP:
            return NOOP
        return self.wrap_cmd(self._cmd(task.input_files, task.output_files, task))

    def __repr__(self):
        return '<Tool[%s] %s %s>' % (id(self), self.name, self.tags)


class Tool_old(Tool):
    """
    Old input/output specification.  Deprecated and will be removed.
    """
    api_version = 1


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

        path = _abs(path)
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


def unpack_taskfiles_with_cardinality_1(odict):
    new = odict.copy()
    for aif, taskfiles in odict.items():
        op, number = parse_aif_cardinality(aif.n)
        if op in ['=', '=='] and number == 1:
            new[aif] = taskfiles[0]
        else:
            new[aif] = taskfiles
    return new


def unpack_if_cardinality_1(aif, taskfiles):
    op, number = parse_aif_cardinality(aif.n)
    if op in ['=', '=='] and number == 1:
        return taskfiles[0]
    else:
        return taskfiles


# def group_taskfiles_by_aif(taskfiles):
# f = lambda tf: tf.abstract_input_file_mapping
# for (aif_index, aif), taskfiles in it.groupby2(sorted(taskfiles, key=f), f):
# print (aif_index, aif)
# taskfiles = list(taskfiles)
# print taskfiles
# op, number = parse_aif_cardinality(aif.n)
# print op, number
# if op in ['=', '=='] and number == 1:
# yield taskfiles[0]
# else:
# yield taskfiles


# class TaskFileDict(dict):
# """
# The `input_dict` and `output_dict` object passed to Tool.cmd()
# """
# format = None
#
# def __init__(self, taskfiles, type):
# assert type in ['input', 'output']
# self.type = type
# self.taskfiles = taskfiles
#         if type == 'input':
#             kwargs = {name: list(input_files) for name, input_files in groupby2(taskfiles, lambda i: i.name)}
#         else:
#             kwargs = {t.name: t for t in taskfiles}  # only have 1 output_file per name
#
#         super(TaskFileDict, self).__init__(**kwargs)
#
#         self.format = {fmt: list(output_files) for fmt, output_files in groupby2(self.taskfiles, lambda i: i.format)}
#
#
#     def __iter__(self):
#         if self.type == 'input':
#             #f = lambda tf: getattr(tf, 'abstract_input_file_mapping', None)
#             f = lambda tf: tf['abstract_input_file_mapping']
#             for (aif_index, aif), taskfiles in it.groupby2(sorted(self.taskfiles, key=f), f):
#                 taskfiles = list(taskfiles)
#                 op, number = parse_aif_cardinality(aif.n)
#                 if op in ['=', '=='] and number == 1:
#                     yield taskfiles[0]
#                 else:
#                     yield taskfiles
#         else:
#             for tf in self.taskfiles:
#                 yield tf
#
#
#     def __getitem__(self, val):
#         # slow, but whatever.
#         return list(self.__iter__())[val]
#
#     def __repr__(self):
#         if len(self.taskfiles) == 1:
#             return self.taskfiles[0].__repr__()
#         else:
#             return '<TaskFileDict>'
#
#     def __str__(self):
#         return self.__repr__()


# # ##
# # Merges multiple tools
# # ##
#
# MergedCommand = namedtuple('MergedCommand', ['results'])
#
# """
# two ways to chain
# 1) merged output is only the last tool's outputs
# 2) merged output is all tool's outputs (requires no overlapping output names, or latter tool gets precedence)
# """
#
#
# class CollapsedTool(Tool):
#     pass
#
#
# #
# #
# def chain(*tool_classes):
#     """
#     Collapses multiple tools down into one, to reduce the number of jobs being submitted and general overhead by reducing the size of a taskgraph.
#
#     :param tool_classes: a iterable of Tools to chain
#     :param name: the name for the class.  Default is '__'.join(tool_classes)
#     :return: (str) a command
#     """
#     global CollapsedTool
#     tool_classes = tuple(tool_classes)
#     assert all(issubclass(tc, Tool) for tc in tool_classes), 'tool_classes must be an iterable of Tool subclasses'
#     #assert not any(t.NOOP for t in tool_classes), 'merging NOOP tool_classes not supported'
#     name = '__'.join(t.name for t in tool_classes)
#
#
#     def _generate_command(self, task):
#         """
#         Generates the command
#         """
#
#         def chained_tools(tool_classes, task):
#             """
#             Instantiate all tools with their correct i/o
#             """
#             all_outputs = task.output_files[:]
#             this_input_taskfiles = task.input_files
#             for tool_class in tool_classes:
#                 tool = tool_class(task.tags)
#
#                 this_output_taskfiles = []
#                 for abstract_output in tool.outputs:
#                     tf = next(_find(all_outputs, abstract_output, True))
#                     this_output_taskfiles.append(tf)
#                     all_outputs.remove(tf)
#
#                 yield tool, this_input_taskfiles, this_output_taskfiles
#                 for abstract_input in tool.inputs:
#                     if abstract_input.forward:
#                         this_output_taskfiles += list(_find(this_input_taskfiles, abstract_input, True))
#                 this_input_taskfiles = this_output_taskfiles
#
#         # def chained_tools(tool_classes, task):
#         # """
#         # Instantiate all tools with their correct i/o
#         # """
#         # all_outputs = task.output_files[:]
#         # this_input_taskfiles = task.input_files
#         # import itertools as it
#         # def map_(input_files, abstract_outputs):
#         # return list(it.chain(*(_find(input_files, aof, True) for aof in abstract_outputs))
#         #
#         # for i, tool in enumerate(tool_classes):
#         #         if i == 0:
#         #             # is first
#         #             yield tool, task.input_files, get_outs(task.input_files, tool.outputs)
#         #         elif i == len(tool_classes) - 1:
#         #             # is last
#         #             yield tool, task.input_files, task.output_files
#
#         cmd = self._prepend_cmd(task)
#         for tool, input_taskfiles, output_taskfiles in chained_tools(self.merged_tool_classes, task):
#             cmd_result = tool._cmd(input_taskfiles, output_taskfiles, task)
#             cmd += '### ' + tool.name + ' ###\n\n'
#             cmd += cmd_result
#             cmd += '\n\n'
#
#         # only keep the last chained Tool's output files
#         # remove = set(task.output_files) - set(output_taskfiles)
#         # for tf in remove:
#         # for ifa in tf._input_file_assocs:
#         #         ifa.delete()
#         #     tf.task_output_for = None
#
#         return cmd
#
#
#     CollapsedTool = type(name, (CollapsedTool,),  # TODO: inherit from the merged tools, but without a metaclass conflict
#                          dict(merged_tool_classes=tool_classes,
#                               _generate_command=_generate_command,
#                               name=name,
#                               # inputs=tool_classes[0].inputs,
#                               outputs=list(it.chain(*(tc.outputs for tc in tool_classes))),
#                               # outputs=tool_classes[-1].outputs,
#                               mem_req=max(t.mem_req for t in tool_classes),
#                               time_req=max(t.time_req for t in tool_classes),
#                               cpu_req=max(t.cpu_req for t in tool_classes),
#                               must_succeed=any(t.must_succeed for t in tool_classes),
#                               persist=any(t.persist for t in tool_classes)
#                          )
#     )
#     return CollapsedTool


def _find(taskfiles, abstract_file, error_if_missing=False):
    """
    find `abstract_file` in `taskfiles`
    :param taskfiles: a list of TaskFiles
    :param abstract_file: an AbstractInputFile or AbstractOutputFile
    :param error_if_missing: raise ValueError if a matching taskfile cannot be found
    :return:
    """
    reg_name, reg_format = re.compile(abstract_file.name), re.compile(abstract_file.format)

    found = False
    for tf in taskfiles:
        if re.match(reg_name, tf.name) and re.match(reg_format, tf.format):
            yield tf
            found = True
    if not found and error_if_missing:
        raise ValueError, 'No taskfile found for %s' % abstract_file