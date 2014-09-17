from inspect import getargspec
import os
import re
import itertools as it

from .. import TaskFile, Task
from ..models.TaskFile import InputFileAssociation
from ..util.helpers import str_format, groupby, has_duplicates, strip_lines


opj = os.path.join


class ToolValidationError(Exception): pass


class _ToolMeta(type):
    def __init__(cls, name, bases, dct):
        cls.name = name
        return super(_ToolMeta, cls).__init__(name, bases, dct)


class Tool(object):
    """
    Essentially a factory that produces Tasks.  It's :meth:`cmd` must be overridden unless it is a NOOP task.
    """
    __metaclass__ = _ToolMeta

    mem_req = None
    time_req = None
    cpu_req = None
    must_succeed = True
    NOOP = False
    persist = False
    drm = None
    skip_profile = False
    inputs = []
    outputs = []
    # if adding another attribute, don't forget to update the chain() method


    def __init__(self, tags):
        """
        :param tags: (dict) A dictionary of tags.
        """
        self.tags = tags
        self.__validate()
        self.load_sources = []


    def __validate(self):
        assert all(i.__class__.__name__ == 'AbstractInputFile' for i in self.inputs), 'Tool.inputs must be instantiated using the `input_taskfile` function'
        assert all(o.__class__.__name__ == 'AbstractOutputFile' for o in self.outputs), 'Tool.outputs must be instantiated using the `output_taskfile` function'

        if has_duplicates([(i.name, i.format) for i in self.inputs]):
            raise ToolValidationError("Duplicate task.inputs detected in {0}".format(self))

        if has_duplicates([(i.name, i.format) for i in self.outputs]):
            raise ToolValidationError("Duplicate task.outputs detected in {0}".format(self))

        argspec = getargspec(self.cmd)
        assert {'i', 'o', 's'}.issubset(argspec.args), 'Invalid %s.cmd signature' % self

        if not set(self.tags.keys()).isdisjoint({'i', 'o', 's'}):
            raise ToolValidationError("'i', 'o', 's' are a reserved names, and cannot be used as a tag keyword")


    def _map_inputs(self, parents):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: [(taskfile, is_forward), ...]
        """
        for abstract_file in self.inputs:
            for p in parents:
                for tf in _find(p.output_files + p.forwarded_inputs, abstract_file, error_if_missing=False):
                    yield tf, abstract_file.forward

    def _generate_task(self, stage, parents, default_drm):
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP']}
        d['drm'] = 'local' if self.drm == 'local' else default_drm

        inputs = list(self._map_inputs(parents))
        task = Task(stage=stage, tags=self.tags, _input_file_assocs=[InputFileAssociation(taskfile=tf, forward=is_forward) for tf, is_forward in inputs], parents=parents,
                    **d)
        task.skip_profile = self.skip_profile

        input_taskfiles, _ = zip(*inputs) if inputs else ([], None)
        input_dict = TaskFileDict(input_taskfiles, type='input')

        # Create output TaskFiles
        for name, format, path in self.load_sources:
            TaskFile(name=name, format=format, path=path, task_output_for=task, persist=True)

        for output in self.outputs:
            name = str_format(output.name, dict(i=input_dict, **self.tags))
            if output.basename is None:
                basename = None
            else:
                basename = str_format(output.basename, dict(name=name, format=output.format, i=input_dict, **self.tags))
            TaskFile(task_output_for=task, persist=self.persist, name=name, format=output.format, basename=basename)

        task.tool = self
        return task

    def _cmd(self, input_taskfiles, output_taskfiles, task, settings):
        """
        Wrapper for self.cmd().  Passes any tags that match parameter keywords of self.cmd as parameters, and does some basic validation.
        """
        argspec = getargspec(self.cmd)
        self.task = task
        params = dict(i=TaskFileDict(input_taskfiles, 'input'), o=TaskFileDict(output_taskfiles, 'output'), s=settings)
        params.update({k: v for k, v in self.tags.items() if k in argspec.args})
        ndefaults = len(argspec.defaults) if argspec.defaults else 0
        for arg in argspec.args[:len(argspec.args) - ndefaults]:
            if arg != 'self' and arg not in params:
                raise AttributeError('%s requires the parameter: `%s`, are you missing a tag?' % (self, arg))

        out = self.cmd(**params)
        assert isinstance(out, str), '%s.cmd did not return a str' % self

        out = re.sub('<TaskFile\[(.*?)\] .+?:(.+?)>', lambda m: m.group(2), out)
        # return strip_lines(out.replace(task.execution.output_dir, '$OUT'))
        return strip_lines(out.replace(task.output_dir, '$OUT'))

    def _prepend_cmd(self, task):
        return 'OUT={out}\n' \
               'cd $OUT\n\n'.format(out=task.output_dir)

    def cmd(self, i, o, s, **kwargs):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict who's values are lists) Input TaskFiles.
        :param o: (dict) Output TaskFiles.
        :param s: (dict) Settings.
        :param kwargs: (dict) Parameters.
        :returns: (str) the text to write into the shell script that gets executed
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def _generate_command(self, task, settings):
        """
        Generates the command
        """
        # check input file cardinalities.
        # TODO: this could be done when TaskFiles are first generated to save some compute, but map_inputs would have to be altered to return inputs mapped to each
        #       abstract input file
        for aif in self.inputs:
            real_count = len(list(_find(task.input_files, aif, error_if_missing=True)))
            import operator

            ops = {"<": operator.lt, "<=": operator.le,
                   ">": operator.gt, ">=": operator.ge,
                   "=": operator.eq, '==':operator.eq}

            def error():
                raise ToolValidationError('%s does not have right number of inputs: `%s`, for %s' % (self, aif.n, aif))

            try:
                required_count = int(aif.n)
                if not required_count == real_count:
                    error()
            except ValueError:
                for k in ops:
                    if aif.n.startswith(k):
                        required_count = int(aif.n.replace(k, ''))
                        if not ops[k](required_count, real_count):
                            error()

        return self._prepend_cmd(task) + self._cmd(task.input_files, task.output_files, task, settings)


from collections import namedtuple

InputSource = namedtuple('InputSource', ['name', 'format', 'root_path'])


class Input(Tool):
    """
    A NOOP Task who's output_files contain a *single* file that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load an input file.  for

    >>> Input(path_to_file,tags={'key':'val'})
    >>> Input(path=path_to_file, name='myfile',format='txt',tags={'key':'val'})
    """

    name = 'Load_Input_Files'

    def __init__(self, path, name=None, format=None, tags=None, *args, **kwargs):
        """
        :param name: the name or keyword for the input file.  defaults to whatever format is set to.
        :param path: the path to the input file
        :param tags: tags for the task that will be generated
        :param format: the format of the input file.  Defaults to the value in `name`
        """
        path = _abs(path)
        if tags is None:
            tags = dict()

        basename = os.path.basename(path)
        if name is None:
            name = os.path.splitext(basename)[0]
        if format is None:
            format = os.path.splitext(basename)[-1][1:]  # remove the '.'

        super(Input, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        self.load_sources.append(InputSource(name, format, path))


class Inputs(Tool):
    """
    An Input File.A NOOP Task who's output_files contain a *multiple* files that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load a set of input file.

    >>> Inputs([('name1','txt',root_path), ('name2','gz',roroot_pathath)], tags={'key':'val'})
    "root_path   name = 'Load_Input_Files'
    """

    def __init__(self, inputs, tags=None, *args, **kwargs):
        """
        """
        if tags is None:
            tags = dict()
        super(Inputs, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        for tpl in inputs:
            self.load_sources.append(InputSource(*tpl))


def _abs(path):
    path2 = os.path.abspath(os.path.expanduser(path))
    assert os.path.exists(path2), '%s path does not exist' % path2
    return path2


class TaskFileDict(dict):
    """
    The `input_dict` and `output_dict` object passed to Tool.cmd()
    """
    format = None

    def __init__(self, taskfiles, type):
        assert type in ['input', 'output']
        self.taskfiles = taskfiles
        if type == 'input':
            kwargs = {name: list(input_files) for name, input_files in groupby(taskfiles, lambda i: i.name)}
        else:
            kwargs = {t.name: t for t in taskfiles}  # only have 1 output_file per name

        super(TaskFileDict, self).__init__(**kwargs)

        self.format = {fmt: list(output_files) for fmt, output_files in groupby(self.taskfiles, lambda i: i.format)}

    def __iter__(self):
        pass


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
class CollapsedTool(Tool):
    pass


#
#
def chain(*tool_classes):
    """
    Collapses multiple tools down into one, to reduce the number of jobs being submitted and general overhead by reducing the size of a taskgraph.

    :param tool_classes: a iterable of Tools to chain
    :param name: the name for the class.  Default is '__'.join(tool_classes)
    :return: (str) a command
    """
    global CollapsedTool
    tool_classes = tuple(tool_classes)
    assert all(issubclass(tc, Tool) for tc in tool_classes), 'tool_classes must be an iterable of Tool subclasses'
    assert not any(t.NOOP for t in tool_classes), 'merging NOOP tool_classes not supported'
    name = '__'.join(t.name for t in tool_classes)


    def _generate_command(self, task, settings):
        """
        Generates the command
        """

        def chained_tools(tool_classes, task):
            """
            Instantiate all tools with their correct i/o
            """
            all_outputs = task.output_files[:]
            this_input_taskfiles = task.input_files
            for tool_class in tool_classes:
                tool = tool_class(task.tags)

                this_output_taskfiles = []
                for abstract_output in tool.outputs:
                    tf = next(_find(all_outputs, abstract_output, True))
                    this_output_taskfiles.append(tf)
                    all_outputs.remove(tf)

                yield tool, this_input_taskfiles, this_output_taskfiles
                for abstract_input in tool.inputs:
                    if abstract_input.forward:
                        this_output_taskfiles += list(_find(this_input_taskfiles, abstract_input, True))
                this_input_taskfiles = this_output_taskfiles

        # def chained_tools(tool_classes, task):
        # """
        #     Instantiate all tools with their correct i/o
        #     """
        #     all_outputs = task.output_files[:]
        #     this_input_taskfiles = task.input_files
        #     import itertools as it
        #     def map_(input_files, abstract_outputs):
        #         return list(it.chain(*(_find(input_files, aof, True) for aof in abstract_outputs))
        #
        #     for i, tool in enumerate(tool_classes):
        #         if i == 0:
        #             # is first
        #             yield tool, task.input_files, get_outs(task.input_files, tool.outputs)
        #         elif i == len(tool_classes) - 1:
        #             # is last
        #             yield tool, task.input_files, task.output_files

        cmd = self._prepend_cmd(task)
        for tool, input_taskfiles, output_taskfiles in chained_tools(self.merged_tool_classes, task):
            cmd_result = tool._cmd(input_taskfiles, output_taskfiles, task, settings)
            cmd += '### ' + tool.name + ' ###\n\n'
            cmd += cmd_result
            cmd += '\n\n'

        # only keep the last chained Tool's output files
        # remove = set(task.output_files) - set(output_taskfiles)
        # for tf in remove:
        # for ifa in tf._input_file_assocs:
        #         ifa.delete()
        #     tf.task_output_for = None

        return cmd


    CollapsedTool = type(name, (CollapsedTool,),  # TODO: inherit from the merged tools, but without a metaclass conflict
                         dict(merged_tool_classes=tool_classes,
                              _generate_command=_generate_command,
                              name=name,
                              # inputs=tool_classes[0].inputs,
                              outputs=list(it.chain(*(tc.outputs for tc in tool_classes))),
                              #outputs=tool_classes[-1].outputs,
                              mem_req=max(t.mem_req for t in tool_classes),
                              time_req=max(t.time_req for t in tool_classes),
                              cpu_req=max(t.cpu_req for t in tool_classes),
                              must_succeed=any(t.must_succeed for t in tool_classes),
                              persist=any(t.persist for t in tool_classes)
                         )
    )
    return CollapsedTool


def _find(taskfiles, abstract_file, error_if_missing=False):
    """
    find `abstract_file` in `taskfiles`
    :param taskfiles: a list of TaskFiles
    :param abstract_file: an AbstractInputFile or AbstractOutputFile
    :param error_if_missing: raise ValueError if a matching taskfile cannot be found
    :return:
    """
    name, format = abstract_file.name, abstract_file.format
    assert name or format

    if format == '*':
        for tf in taskfiles:
            yield tf
    else:
        found = False
        for tf in taskfiles:
            if name in [tf.name, None] and format in [tf.format, None]:
                yield tf
                found = True
        if not found and error_if_missing:
            raise ValueError, 'No taskfile found with name=%s, format=%s' % (name, format)