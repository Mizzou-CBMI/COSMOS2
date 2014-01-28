from inspect import getargspec, getcallargs
import os
import re
from .. import TaskFile, Task
from ..util.helpers import parse_cmd, kosmos_format, groupby


opj = os.path.join


class ToolValidationError(Exception): pass


class Tool(object):
    """
    Essentially a factory that produces Tasks.  It's :meth:`cmd` must be overridden unless it is a NOOP task.
    """
    mem_req = None
    time_req = None
    cpu_req = None
    must_succeed = None
    NOOP = False
    persist = False

    def __init__(self, tags, *args, **kwargs):
        """
        :param tags: (dict) A dictionary of tags.
        :param stage: (str) The stage this task belongs to.
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
        self.tags=tags

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
                return {'*': [o for p in parents for o in p.taskfiles]}

            all_inputs = filter(lambda x: x is not None,
                                [p.get_output(name, error_if_missing=False) for p in parents for name in
                                 self.inputs])

            input_names = [i.name for i in all_inputs]
            for k in self.inputs:
                if k not in input_names:
                    raise ValueError("Could not find input '{0}' for {1}".format(k, self))

            return all_inputs

    def generate_task(self, parents, settings, parameters):
        # Create output TaskFiles
        output_files = []
        for output in self.outputs:
            if isinstance(output, tuple):
                tf = TaskFile(name=output[0], basename=output[1].format(name=output[0], **self.tags), persist=self.persist)
            elif isinstance(output, str):
                tf = TaskFile(name=output, persist=self.persist)
            else:
                raise ToolValidationError("{0}.outputs must be a list of strs or tuples.".format(self))
            output_files.append(tf)
        if isinstance(self, Input):
                output_files.append(TaskFile(name=self.input_name, path=self.input_path, persist=True))
        elif isinstance(self, Inputs):
            for name, path in self.input_args:
                output_files.append(TaskFile(name=name, path=path, persist=True))

        d = {attr: getattr(self, attr) for attr in
             ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP']}
        task = Task(tags=self.tags, output_files=output_files, input_files=self.map_inputs(parents), parents=parents,
                    forward_inputs=self.forward_inputs,
                    **d)
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
        #TODO this copy probably can be removed
        p = self.parameters.copy()
        argspec = getargspec(self.cmd)

        # add tags to params
        for k, v in task.tags.items():
            if k in argspec.args:
                p[k] = v

        for k in p:
            if k not in argspec.args:
                del p[k]

        for l in ['i', 'o', 's']:
            if l in p.keys():
                raise ToolValidationError, "{0} is a reserved name, and cannot be used as a tag keyword".format(l)

        try:
            input_dict = {name: list(input_files) for name, input_files in groupby(task.input_files, lambda i: i.name)}
            kwargs = dict(i=input_dict, o={o.name: o for o in task.output_files}, s=self.settings, **p)
            callargs = getcallargs(self.cmd, **kwargs)
        except TypeError:
            raise TypeError, 'Invalid parameters for {0}.cmd(): {1}'.format(self, kwargs.keys())

        del callargs['self']
        r = self.cmd(**callargs)

        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 and r else {}
        pcmd = r[0] if len(r) == 2 else r

        #format() return string with callargs
        callargs['self'] = self
        callargs['task'] = task
        callargs.update(extra_format_dict)
        cmd = kosmos_format(pcmd, callargs)

        #fix TaskFiles paths
        cmd = re.sub('<TaskFile\[\d+?\] .+?:(.+?)>', lambda x: x.group(1), cmd)

        return parse_cmd(cmd)


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

    Does not actually execute anything, but provides a way to load an input file.

    >>> Input('ext','/path/to/file.ext',tags={'key':'val'})
    >>> Input(path='/path/to/file.ext.gz',name='ext',tags={'key':'val'})
    """

    name = 'Load_Input_Files'
    def __init__(self, name, path, tags, *args, **kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        path = os.path.abspath(path)
        super(Input, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        # if name is None:
        #     _, name = os.path.splitext(path)
        #     name = name[1:] # remove '.'
        #     assert name != '', 'name not specified, and path has no extension'

        self.input_path = path
        self.input_name = name


class Inputs(Tool):
    """
    An Input File.A NOOP Task who's output_files contain a *multiple* files that already exists on the filesystem.

    Does not actually execute anything, but provides a way to load a set of input file.

    >>> Input('ext','/path/to/file.ext',tags={'key':'val'})
    >>> Input(path='/path/to/file.ext.gz',name='ext',tags={'key':'val'})
    """
    name = 'Load_Input_Files'
    def __init__(self, inputs, tags, *args, **kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        #path = os.path.abspath(path)
        super(Inputs, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True

        self.input_args = inputs