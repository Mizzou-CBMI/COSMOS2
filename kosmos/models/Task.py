import itertools as it
import copy
from inspect import getargspec, getcallargs
import os
import json

from ..helpers import parse_cmd, cosmos_format
from .TaskFile import TaskFile

opj = os.path.join

i = 0


def get_id():
    global i
    i += 1
    return i


files = []


class ExpectedError(Exception): pass
class ToolError(Exception): pass
class ToolValidationError(Exception): pass
class GetOutputError(Exception): pass

from ..signals import task_finished


def rcv_task_finished(task):
    task.is_finished = True


task_finished.connect(rcv_task_finished)


class Task(object):
    """
    A Tool is a class who's instances represent a command that gets executed.  It also contains properties which
    define the resources that are required.

    :property stage: (str) The Tool's Stage
    :property dag: (TaskGraph) The dag that is keeping track of this Tool
    :property id: (int) A unique identifier.  Useful for debugging.
    :property input_files: (list) This Tool's input TaskFiles
    :property output_files: (list) This Tool's output TaskFiles.  A task's output taskfile names should be unique.
    :property tags: (dict) This Tool's tags.
    """
    #TODO props that cant be overridden should be private

    #: (int) Number of megabytes of memory to request.  Default is 1024.
    mem_req = 1 * 1024
    #: (int) Number of cores to request.  Default is 1.
    cpu_req = 1
    #: (int) Number of minutes to request. Default is 1.
    time_req = None #(mins)
    #: (bool) If True, these tasks do not contain commands that are executed.  Used for INPUT.  Default is False.
    NOOP = False
    #: (bool) If True, if this task's tasks' job attempts fail, the task will still be considered successful.  Default is False.
    succeed_on_failure = False
    #: (bool) If True, output_files described as a str in outputs will be by default be created with persist=True.
    #: If delete_interemediates is on, they will not be deleted.
    persist = False
    #: forwards this task's input to get_output() calls
    # forward_input = False
    #: always run job as a subprocess even when DRM is not set to local
    always_local = False
    log_dir = None
    output_dir = None

    @property
    def output_profile_path(self):
        assert self.log_dir is not None
        return opj(self.log_dir, 'profile.json')

    @property
    def output_command_script_path(self):
        assert self.log_dir is not None
        return opj(self.log_dir, 'command.bash')

    @property
    def output_stderr_path(self):
        assert self.log_dir is not None
        return opj(self.log_dir, 'stderr.txt')

    @property
    def output_stdout_path(self):
        assert self.log_dir is not None
        return opj(self.log_dir, 'stdout.txt')


    def __init__(self, tags, stage=None, dag=None):
        """
        :param stage: (str) The stage this task belongs to. Required.
        :param tags: (dict) A dictionary of tags.
        :param dag: The dag this task belongs to.
        :param parents: A list of task instances which this task is dependent on
        """
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tasks should have at least one tag.')
        self.output_files = []
        self.settings = {}
        self.parameters = {}
        if not hasattr(self, 'name'): self.name = self.__class__.__name__
        if not hasattr(self, 'inputs'): self.inputs = []
        if not hasattr(self, 'outputs'): self.outputs = []
        self.is_finished = False

        self.stage = stage
        self.dag = dag
        self.tags = {k: str(v) for k, v in tags.copy().items()}

        # Because defining attributes in python creates a reference to a single instance across all class instance
        # any taskfile instances in self.outputs is used as a template for instantiating a new class
        self.outputs = [copy.copy(o) if isinstance(o, TaskFile) else o for o in self.outputs]
        self.id = get_id()

        # Create empty output TaskFiles
        for output in self.outputs:
            if isinstance(output, TaskFile):
                self.output_files.append(output)
            elif isinstance(output, str):
                tf = TaskFile(name=output, task=self)
                self.output_files.append(tf)
            else:
                raise ToolValidationError, "{0}.outputs must be a list of strs or Taskfile instances.".format(self)

        #validate inputs are strs
        if any([not isinstance(i, str) for i in self.inputs]):
            raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError(
                'Duplicate names in task.inputs detected in {0}.  Perhaps try using [1.ext,2.ext,...]'.format(self))

        output_names = [o.name for o in self.output_files]
        if len(output_names) != len(set(output_names)):
            raise ToolValidationError(
                'Duplicate names in task.output_files detected in {0}.  Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(
                    self))

    @property
    def children(self):
        return self.dag.task_G.successors(self)

    @property
    def parents(self):
        return self.dag.task_G.predecessors(self)

    @property
    def parent(self):
        if len(self.parents) > 1:
            raise ToolError('{0} has more than one parent.  The parents are: {1}'.format(self, self.parents))
        elif len(self.parents) == 0:
            raise ToolError('{0} has no parents'.format(self))
        else:
            return self.parents[0]

    def get_output(self, name, error_if_missing=True):
        for o in self.output_files:
            if o.name == name:
                return o

        if error_if_missing:
            raise ToolError, 'Output named `{0}` does not exist in {1}'.format(name, self)

    @property
    def input_files(self):
        "A list of input TaskFiles"
        return list(it.chain(*[tf for tf in self.map_inputs().values()]))

    def get_profile_output(self):
        if not os.path.exists(self.output_profile_path):
            return {}
        else:
            with open(self.output_profile_path, 'r') as fh:
                return json.load(fh)

    @property
    def label(self):
        "Label used for the TaskGraph image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format(
            "\\n".join(["{0}: {1}".format(k, v) for k, v in self.tags.items()]))
        return "[{3}] {0}{1}".format(self.name, tags, self.pcmd, self.id)

    def map_inputs(self):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: (dict) A dictionary of taskfiles which are inputs to this task.  Keys are names of the taskfiles, values are a list of taskfiles.
        """
        if not self.inputs:
            return {}

        else:
            if '*' in self.inputs:
                return {'*': [o for p in self.parents for o in p.output_files]}

            all_inputs = filter(lambda x: x is not None,
                                [p.get_output(name, error_if_missing=False) for p in self.parents for name in
                                 self.inputs])

            input_dict = dict(
                (name, list(input_files)) for name, input_files in it.groupby(all_inputs, lambda i: i.name))

            for k in self.inputs:
                if k not in input_dict or len(input_dict[k]) == 0:
                    raise ToolValidationError, "Could not find input '{0}' for {1}".format(k, self)

            return input_dict


    def generate_cmd(self):
        """
        Calls map_inputs() and processes the output of cmd()
        """
        p = self.parameters.copy()
        argspec = getargspec(self.cmd)

        for k, v in self.tags.items():
            if k in argspec.args:
                p[k] = v

        ## Validation
        # Helpful error message
        if not argspec.keywords: #keywords is the **kwargs name or None if not specified
            for k in p.keys():
                if k not in argspec.args:
                    raise ToolValidationError, '{0} received the parameter "{1}" which is not defined in it\'s signature.  Parameters are {2}.  Accept the parameter **kwargs in cmd() to generalize the parameters accepted.'.format(
                        self, k, argspec.args)

        for l in ['i', 'o', 's']:
            if l in p.keys():
                raise ToolValidationError, "{0} is a reserved name, and cannot be used as a tag keyword".format(l)

        try:
            kwargs = dict(i=self.map_inputs(), o={o.name: o for o in self.output_files}, s=self.settings, **p)
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
        callargs.update(extra_format_dict)
        return parse_cmd(cosmos_format(pcmd, callargs))


    def cmd(self, i, s, o, p):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict) Input TaskFiles.
        :param s: (dict) Settings.  The settings dictionary, set by using :py:meth:`lib.ezflow.dag.configure`
        :param p: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))


    def configure(self, settings={}, parameters={}):
        """
        """
        self.parameters = parameters
        self.settings = settings
        return self


    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.tags)


class INPUT(Task):
    """
    An Input File.

    Does not actually execute anything, but provides a way to load an input file.

    >>> INPUT('/path/to/file.ext',tags={'key':'val'})
    >>> INPUT(path='/path/to/file.ext.gz',name='ext',fmt='ext.gz',tags={'key':'val'})
    """
    name = "Load_Input_Files"
    NOOP = True
    mem_req = 0
    cpu_req = 0
    persist = True

    def __init__(self, path, tags, name=None, fmt=None, *args, **kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        path = os.path.abspath(path)
        super(INPUT, self).__init__(tags=tags, *args, **kwargs)
        self.output_files.append(TaskFile(path=path, name=name, task=self))

    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id, self.__class__.__name__, self.tags)