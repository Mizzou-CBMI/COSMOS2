from inspect import getargspec, getcallargs
import os
import re
import itertools as it

from .. import TaskFile, Task
from ..util.helpers import strip_lines, str_format, groupby, has_duplicates


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
    must_succeed = None
    NOOP = False
    persist = False
    drm = None
    # if adding another attribute, don't forget to update the merge() method


    def __init__(self, tags, *args, **kwargs):
        """
        :param tags: (dict) A dictionary of tags.
        """
        if not hasattr(self, 'inputs'): self.inputs = []
        if not hasattr(self, 'outputs'): self.outputs = []
        if not hasattr(self, 'settings'): self.settings = {}
        if not hasattr(self, 'parameters'): self.parameters = {}
        if not hasattr(self, 'forward_inputs'): self.forward_inputs = []

        # TODO validate tags are strings and 1 level
        # self.tags = {k: str(v) for k, v in self.tags.items()}
        self.tags = tags

        self._validate()

    def map_inputs(self, parents):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: (list) a list of input taskfiles
        """
        if not self.inputs:
            return []

        else:
            if '*' in self.inputs:
                return {'*': [tf for p in parents for tf in p.all_outputs()]}

            all_inputs = [tf for input_args in self.inputs for p in parents for tf in p.get_outputs(input_args['name'], input_args['format'], error_if_missing=False)]
            input_names = [i.name for i in all_inputs]
            input_formats = [i.format for i in all_inputs]

            for k in self.inputs:
                if k['name'] not in input_names and k['format'] not in input_formats:
                    raise ValueError("Could not find input '{0}' for {1}.  Parents are {2}".format(k, self, parents))

            return all_inputs

    def generate_task(self, stage, parents, settings, parameters, drm):
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP']}
        input_files = self.map_inputs(parents)
        input_dict = TaskFileDict(input_files, type='input')
        task = Task(stage=stage, tags=self.tags, input_files=input_files, parents=parents, drm=drm, forward_inputs=self.forward_inputs, **d)

        # Create output TaskFiles
        output_files = []
        for output in self.outputs:
            assert hasattr(output, '_output_taskfile'), 'Tool outputs must be instantiated using the `output_taskfile` function'
            name = str_format(output['name'], dict(i=input_dict, **self.tags))
            if output['basename'] is not None:
                basename = str_format(output['basename'], dict(name=name, format=output.format, i=input_dict, **self.tags))
            else:
                basename = output['basename']


            output_files.append(TaskFile(task_output_for=task, persist=self.persist, name=name, format=output['format'], basename=basename))
        if isinstance(self, Input):
            output_files.append(TaskFile(name=self.name, format=self.format, path=self.path, task_output_for=task, persist=True))
        elif isinstance(self, Inputs):
            for name, path, format in self.input_args:
                output_files.append(TaskFile(name=name, format=format, path=path, task_output_for=task, persist=True))

        task.tool = self
        self.settings = settings
        self.parameters = parameters

        return task

    def cmd(self, i, o, s, **kwargs):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict who's values are lists) Input TaskFiles.
        :param o: (dict) Output TaskFiles.
        :param s: (dict) Settings.
        :param kwargs: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def generate_command(self, task):
        """

        """
        argspec = getargspec(self.cmd)

        for k in self.parameters:
            if k not in argspec.args:
                raise ToolValidationError('Parameter %s is not a part of the %s.cmd signature' % (k, self))

        p = self.parameters.copy()

        if {'inputs', 'outputs', 'settings'}.issubset(argspec.args):
            signature_type = 'A'
        elif {'i', 'o', 's'}.issubset(argspec.args):
            signature_type = 'B'
        else:
            raise ToolValidationError('Invalid %s.cmd signature'.format(self))


        # add tags to params
        p.update({k: v for k, v in task.tags.items() if k in argspec.args})

        for l in ['i', 'o', 's', 'inputs', 'outputs', 'settings']:
            if l in p.keys():
                raise ToolValidationError("%s is a reserved name, and cannot be used as a tag keyword" % l)

        input_dict = TaskFileDict(task.input_files, type='input')
        output_dict = TaskFileDict(task.output_files, type='output')

        try:
            if signature_type == 'A':
                kwargs = dict(inputs=input_dict, outputs=output_dict, settings=self.settings, **p)
            elif signature_type == 'B':
                kwargs = dict(i=input_dict, o=output_dict, s=self.settings, **p)
            callargs = getcallargs(self.cmd, **kwargs)

        except TypeError:
            raise TypeError('Invalid parameters for {0}.cmd(): {1}'.format(self, kwargs.keys()))

        del callargs['self']
        r = self.cmd(**callargs)

        # if tuple is returned, second element is a dict to format with
        pcmd, extra_format_dict = (r[0], r[1]) if isinstance(r, tuple) and len(r) == 2 else (r, {})

        # format() return string with callargs
        callargs['self'] = self
        callargs['task'] = task
        callargs.update(extra_format_dict)
        cmd = str_format(strip_lines(pcmd), callargs)

        #fix TaskFiles paths
        cmd = re.sub('<TaskFile\[\d+?\] .+?:(.+?)>', lambda x: x.group(1), cmd)
        assert 'TaskFile' not in cmd, 'An error occurred in the TaskFile regular expression replacement:\n %s' % cmd

        return 'set -e\n\n' + strip_lines(cmd)


    def _validate(self):
        # validate inputs are strs
        # if any([not isinstance(i, str) for i in self.inputs]):
        # raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        assert all(hasattr(i, '_input_taskfile') for i in self.inputs), 'Tool.inputs must be instantiated using the `input_taskfile` function'
        assert all(hasattr(o, '_output_taskfile') for o in self.outputs), 'Tool.outputs must be instantiated using the `output_taskfile` function'

        if has_duplicates([(i.name, i.format) for i in self.inputs]):
            raise ToolValidationError("Duplicate task.inputs detected in {0}".format(self))

        if has_duplicates([(i.name, i.format) for i in self.outputs]):
            raise ToolValidationError("Duplicate task.outputs detected in {0}".format(self))


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
        assert type in ['input','output']
        if type == 'input':
            kwargs = {name: list(input_files) for name, input_files in groupby(taskfiles, lambda i: i.name)}
        else:
            kwargs = {t.name: t for t in taskfiles}  # only have 1 output_file per name

        super(TaskFileDict, self).__init__(**kwargs)
        self.format = {fmt: list(output_files) for fmt, output_files in groupby(taskfiles, lambda i: i.format)}