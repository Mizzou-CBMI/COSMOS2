from inspect import getargspec
import os
import re
import itertools as it

from .. import TaskFile, Task
from ..models.TaskFile import InputFileAssociation
from ..util.helpers import str_format, groupby, has_duplicates, strip_lines
from recordtype import recordtype


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
    inputs = []
    outputs = []
    # if adding another attribute, don't forget to update the collapse_tools() method


    def __init__(self, tags):
        """
        :param tags: (dict) A dictionary of tags.
        """
        self.tags = tags
        self.__validate()

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
        :returns: (list) a list of input taskfiles
        """
        if '*' in self.inputs:
            return {'*': [tf for p in parents for tf in p.all_outputs()]}

        l = []
        for abstract_file in self.inputs:
            for p in parents:
                for tf in _find(p.all_outputs, abstract_file, error_if_missing=False):
                    l.append(tf)
        return l


    def _generate_task(self, stage, parents, default_drm):
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP']}
        input_files = self._map_inputs(parents)
        input_dict = TaskFileDict(input_files, type='input')
        drm = 'local' if self.drm == 'local' else default_drm
        task = Task(stage=stage, tags=self.tags, input_file_assoc=[InputFileAssociation(taskfile=i) for i in input_files], parents=parents, drm=drm, **d)

        # Create output TaskFiles
        output_files = []
        if isinstance(self, Input):
            output_files.append(TaskFile(name=self.name, format=self.format, path=self.path, task_output_for=task, persist=True))
        elif isinstance(self, Inputs):
            for name, path, format in self.input_args:
                output_files.append(TaskFile(name=name, format=format, path=path, task_output_for=task, persist=True))
        else:
            for output in self.outputs:
                name = str_format(output.name, dict(i=input_dict, **self.tags))
                if output.basename is not None:
                    basename = str_format(output.basename, dict(name=name, format=output.format, i=input_dict, **self.tags))
                else:
                    basename = output.basename

                output_files.append(TaskFile(task_output_for=task, persist=self.persist, name=name, format=output.format, basename=basename))

        task.tool = self
        return task

    def _cmd(self, input_taskfiles, output_taskfiles, task, settings):
        """
        Wrapper fir self.cmd().  Passes any tags that match parameter keywords of self.cmd as parameters, and does some basic validation.  Also prepends the bash script
        with some basic things, like 'set -e' and setting the cwd.
        """
        argspec = getargspec(self.cmd)
        self.task = task
        params = dict(i=TaskFileDict(input_taskfiles, 'input'), o=TaskFileDict(output_taskfiles, 'output'), s=settings)
        params.update({k: v for k, v in self.tags.items() if k in argspec.args})
        out = self.cmd(**params)
        assert isinstance(out, str), '%s.cmd did not return a str' % self

        out = re.sub('<TaskFile\[\d+?\] .+?:(.+?)>', lambda m: m.group(1), out)
        return strip_lines(out.replace(task.execution.output_dir, '$OUT'))

    def _prepend_cmd(self, task):
        return 'set -e\n' \
               'OUT={ex_out}\n' \
               'cd {cd}\n\n'.format(cd=task.output_dir.replace(task.execution.output_dir, '$OUT'),
                                    ex_out=task.execution.output_dir)

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

    def generate_command(self, task, settings):
        """
        Generates the command
        """
        return self._prepend_cmd(task) + self._cmd(task.input_files, task.output_files, task, settings)


class Input(Tool):
    """
    A NOOP Task who's output_files contain a *single* file that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load an input file.  for

    >>> Input('txt','/path/to/name.txt',tags={'key':'val'})
    >>> Input(path='/path/to/name.format.gz',name='name',format='format',tags={'key':'val'})
    """

    name = 'Load_Input_Files'

    def __init__(self, name, format, path, tags=None, *args, **kwargs):
        """
        :param name: the name or keyword for the input file.  defaults to whatever format is set to.
        :param path: the path to the input file
        :param tags: tags for the task that will be generated
        :param format: the format of the input file.  Defaults to the value in `name`
        """
        path = _abs(path)
        if tags is None:
            tags = dict()
        super(Input, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True

        self.name = name
        self.format = format
        self.path = path
        self.tags = tags


class Inputs(Tool):
    """
    An Input File.A NOOP Task who's output_files contain a *multiple* files that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load a set of input file.

    >>> Inputs([('name1','/path/to/name.format', 'format'), ('name2','/path/to/name.format2.gz')], tags={'key':'val'})
    """
    name = 'Load_Input_Files'

    def __init__(self, inputs, tags=None, *args, **kwargs):
        """
        """
        if tags is None:
            tags = dict()
            # path = os.path.abspath(path)
        super(Inputs, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        inputs = [(tpl[0], _abs(tpl[1]), tpl[2] if len(tpl) > 2 else tpl[0]) for tpl in inputs]
        self.input_args = inputs


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

    @property
    def format(self):
        return {fmt: list(output_files) for fmt, output_files in groupby(self.taskfiles, lambda i: i.format)}


# ##
# Merges multiple tools
###

MergedCommand = recordtype('MergedCommand', ['results'])

"""
two ways to merge
1) merged output is only the last tool's outputs
2) merged output is all tool's outputs (requires no overlapping output names, or latter tool gets precedence)
"""


class CollapsedTool(Tool):
    pass


def collapse_tools(*tool_classes):
    """
    Collapses multiple tools down into one, to reduce the number of jobs being submitted and general overhead by reducing the size of a taskgraph.

    :param tool_classes: a iterable of Tools to collapse
    :param name: the name for the class.  Default is '__'.join(tool_classes)
    :return: A MergedCommand, which is a record with attributes results.  Results is a list of elements that are either (str, dict) or just a str.
    """
    global CollapsedTool
    tool_classes = tuple(tool_classes)
    assert all(issubclass(tc, Tool) for tc in tool_classes), 'tool_classes must be an iterable of subclasses of Tool'
    assert not any(t.NOOP for t in tool_classes), 'merging NOOP tool_classes not supported'
    name = '__'.join(t.name for t in tool_classes)

    def generate_command(self, task, settings):
        """
        Generates the command
        """
        output_files = task.output_files

        def instantiate_tools(tool_classes, input_init):
            # instantiate all tools with their correct i/o
            all_outputs = task.output_files[:]
            this_inputs = input_init
            for tool_class in tool_classes:
                tool = tool_class(self.tags)

                this_outputs = []
                for abstract_output in tool.outputs:
                    tf = next(_find(all_outputs, abstract_output, True))
                    this_outputs.append(tf)
                    all_outputs.remove(tf)

                yield tool, this_inputs, this_outputs
                for abstract_input in tool.inputs:
                    if abstract_input.forward:
                        this_outputs += list(_find(this_inputs, abstract_input, True))
                this_inputs = this_outputs

        s = self._prepend_cmd(task)
        for tool, inputs, outputs in instantiate_tools(self.merged_tool_classes, task.input_files):
            cmd_result = tool._cmd(inputs, outputs, task, settings)
            s += '### ' + tool.name + ' ###\n\n'
            s += cmd_result
            s += '\n\n'

        return s


    CollapsedTool = type(name, (CollapsedTool,),  # TODO: inherit from the merged tools, but without a metaclass conflict
                         dict(merged_tool_classes=tool_classes,
                              generate_command=generate_command,
                              name=name,
                              inputs=tool_classes[0].inputs,
                              outputs=list(it.chain(*(t.outputs for t in tool_classes))),
                              mem_req=max(t.mem_req for t in tool_classes),
                              time_req=max(t.time_req for t in tool_classes),
                              cpu_req=max(t.cpu_req for t in tool_classes),
                              must_succeed=any(t.must_succeed for t in tool_classes),
                              persist=any(t.persist for t in tool_classes)
                         )
    )
    return CollapsedTool


def _find(taskfiles, abstract_file, error_if_missing=False):
    name, format = abstract_file.name, abstract_file.format
    assert name or format
    found = False
    for tf in taskfiles:
        if name in [tf.name, None] and format in [tf.format, None]:
            yield tf
            found = True
    if not found and error_if_missing:
        raise ValueError, 'No taskfile found with name=%s, format=%s' % (name, format)