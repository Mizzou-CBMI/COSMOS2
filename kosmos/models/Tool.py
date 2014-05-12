from inspect import getargspec, getcallargs
import os
import re

from .. import TaskFile, Task, taskfile
from ..util.helpers import strip_lines, kosmos_format, groupby


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
    always_local = False


    def __init__(self, tags, *args, **kwargs):
        """
        :param tags: (dict) A dictionary of tags.
        """

        #
        # #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tasks should have at least one tag.')

        if not hasattr(self, 'inputs'): self.inputs = []
        if not hasattr(self, 'outputs'): self.outputs = []
        if not hasattr(self, 'settings'): self.settings = {}
        if not hasattr(self, 'parameters'): self.parameters = {}
        if not hasattr(self, 'forward_inputs'): self.forward_inputs = []

        #TODO validate tags are strings and 1 level
        #self.tags = {k: str(v) for k, v in self.tags.items()}
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

            all_inputs = [tf for name in self.inputs for p in parents for tf in p.get_outputs(name, error_if_missing=False)]
            input_names = [i.name for i in all_inputs]

            for k in self.inputs:
                if k not in input_names:
                    raise ValueError("Could not find input '{0}' for {1}".format(k, self))

            return all_inputs

    def generate_task(self, stage, parents, settings, parameters, drm):
        d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP', 'always_local']}
        input_files = self.map_inputs(parents)
        input_dict = {format: list(input_files) for format, input_files in groupby(input_files, lambda i: i.format)}
        input_dict = _add_names_to_input_dict(input_dict)
        task = Task(stage=stage, tags=self.tags, input_files=input_files, parents=parents, drm=drm, forward_inputs=self.forward_inputs, **d)

        # Create output TaskFiles
        output_files = []
        for output in self.outputs:
            output.basename = output.basename.format(i=input_dict, **self.tags)
            assert isinstance(output, taskfile), 'outputs must be instances of a `taskfile` namedtuple'
            tf = TaskFile(task_output_for=task, persist=self.persist, **output)
            output_files.append(tf)
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

        try:
            input_dict = {name: list(input_files) for name, input_files in groupby(task.input_files, lambda i: i.name)}
            if signature_type == 'A':
                kwargs = dict(inputs=input_dict, outputs={o.name: o for o in task.output_files}, settings=self.settings, **p)
            elif signature_type == 'B':
                kwargs = dict(i=input_dict, o={o.name: o for o in task.output_files}, s=self.settings, **p)
            callargs = getcallargs(self.cmd, **kwargs)
        except TypeError:
            raise TypeError('Invalid parameters for {0}.cmd(): {1}'.format(self, kwargs.keys()))

        del callargs['self']
        r = self.cmd(**callargs)

        #if tuple is returned, second element is a dict to format with
        pcmd, extra_format_dict = (r[0], r[1]) if isinstance(r, tuple) and len(r) == 2 else (r, {})

        #format() return string with callargs
        callargs['self'] = self
        callargs['task'] = task
        callargs.update(extra_format_dict)
        cmd = kosmos_format(pcmd, callargs)

        #fix TaskFiles paths
        cmd = re.sub('<TaskFile\[\d+?\] .+?:(.+?)>', lambda x: x.group(1), cmd)
        assert 'TaskFile' not in cmd, 'An error occurred in the TaskFile regular expression replacement:\n %s' % cmd

        return 'set -e\n\n' + strip_lines(cmd)


    def _validate(self):
        #validate inputs are strs
        if any([not isinstance(i, str) for i in self.inputs]):
            raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError(
                'Duplicate names in task.inputs detected in {0}.  Perhaps try using [1.ext,2.ext,...]'.format(self))

        if len(self.outputs) != len(set(self.outputs)):
            raise ToolValidationError(
                'Duplicate names in task.taskfiles detected in {0}.'
                '  Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(self))


class Input(Tool):
    """
    A NOOP Task who's output_files contain a *single* file that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load an input file.  for

    >>> Input('txt','/path/to/name.txt',tags={'key':'val'})
    >>> Input(path='/path/to/name.format.gz',name='name',format='format',tags={'key':'val'})
    """

    name = 'Load_Input_Files'

    def __init__(self, name, path, tags, format=None, *args, **kwargs):
        """
        :param name: the name or keyword for the input file.  defaults to whatever format is set to.
        :param path: the path to the input file
        :param tags: tags for the task that will be generated
        :param format: the format of the input file.  Defaults to the value in `name`
        """
        path = _abs(path)
        super(Input, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True

        self.name = name
        self.format = format or name
        self.path = path


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
            #path = os.path.abspath(path)
        super(Inputs, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        inputs = [(tpl[0], _abs(tpl[1]), tpl[2] if len(tpl) > 2 else tpl[0]) for tpl in inputs]
        self.input_args = inputs

def _abs(path):
    path2 = os.path.abspath(path)
    assert os.path.exists(path2), '%s path does not exist' % path2
    return path2

class TaskFileDict(dict):
    def __init__(self, *args, **kwargs):
        self.names = dict()
        super(TaskFileDict, self).__init__(*args, **kwargs)

def _add_names_to_input_dict(input_dict):
    "adds the .names attribute the dict `input_dict`, which provides access to taskfiles by name"
    import itertools as it
    d = TaskFileDict(**input_dict)
    d.names = {name: list(input_files) for name, input_files in groupby(it.chain(d.values()), lambda i: i.name)}
    return d

